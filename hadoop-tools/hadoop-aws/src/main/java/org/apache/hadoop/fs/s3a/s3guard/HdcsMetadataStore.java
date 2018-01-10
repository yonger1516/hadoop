package org.apache.hadoop.fs.s3a.s3guard;

import com.amazonaws.AmazonClientException;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.Tristate;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.fs.s3a.S3AUtils.translateException;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.IS_DELETED;

public class HdcsMetadataStore implements MetadataStore {
  private static final Logger LOG = LoggerFactory.getLogger(HdcsMetadataStore
          .class);

  private S3AFileSystem s3afs;
  private Configuration conf;
  private static JedisPool jedisPool;
  private static String redisHost;
  private String username;
  private static AtomicInteger total=new AtomicInteger(0);
  private static AtomicInteger hit=new AtomicInteger(0);

  @Override
  public void initialize(FileSystem fs) throws IOException {
    Preconditions.checkNotNull(fs);
    this.s3afs = (S3AFileSystem) fs;

    //String bucket = s3afs.getBucket();
    conf = s3afs.getConf();
    redisHost = conf.get("redis.host");
    username = s3afs.getUsername();
    initHdcs();
  }

  private void initHdcs() {
    JedisPoolConfig config=new JedisPoolConfig();
    config.setMaxTotal(50);
    config.setMaxIdle(50);
    config.setMinIdle(10);
    config.setTestOnBorrow(true);
    jedisPool=new JedisPool(config,redisHost);
  }

  @Override
  public void initialize(Configuration conf) throws IOException {
    this.conf = conf;
    username = UserGroupInformation.getCurrentUser().getShortUserName();
    initHdcs();
  }

  @Override
  public void delete(Path path) throws IOException {
    doDelete(path, false, true);
  }

  @Override
  public void forgetMetadata(Path path) throws IOException {
    doDelete(path, false, false);
  }

  @Override
  public void deleteSubtree(Path path) throws IOException {
    doDelete(path, true, true);
  }

  @Override
  public PathMetadata get(Path path) throws IOException {
    return get(path, false);
  }

  @Override
  public PathMetadata get(Path path, boolean wantEmptyDirectoryFlag) throws IOException {
    total.addAndGet(1);
    path = checkPath(path);
    LOG.debug("Get Metadata from path {} ", path);

    try {
      final PathMetadata meta;
      if (path.isRoot()) {
        // Root does not persist in the table
        meta = new PathMetadata(makeDirStatus(username, path));
      } else {

        meta = hashToPathMetadata(path, username);
        LOG.debug("Get from path {} returning for : {}",
                 path, meta);
      }

      /*if (wantEmptyDirectoryFlag && meta != null) {
        final FileStatus status = meta.getFileStatus();
        // for directory, we query its direct children to determine isEmpty bit
        if (status.isDirectory()) {
          final QuerySpec spec = new QuerySpec()
                  .withHashKey(pathToParentKeyAttribute(path))
                  .withConsistentRead(true)
                  .withFilterExpression(IS_DELETED + " = :false")
                  .withValueMap(deleteTrackingValueMap);
          final ItemCollection<QueryOutcome> items = table.query(spec);
          boolean hasChildren = items.iterator().hasNext();
          // When this class has support for authoritative
          // (fully-cached) directory listings, we may also be able to answer
          // TRUE here.  Until then, we don't know if we have full listing or
          // not, thus the UNKNOWN here:
          meta.setIsEmptyDirectory(
                  hasChildren ? Tristate.FALSE : Tristate.UNKNOWN);
        }
      }*/

      return meta;
    } catch (AmazonClientException e) {
      throw translateException("get", path, e);
    }
  }

  private PathMetadata hashToPathMetadata(Path path, String username) {
    Jedis redis=null;
    try {
       redis= jedisPool.getResource();
      Map<String, String> fields = redis.hgetAll(path.toString());
      if (null == fields || fields.size() == 0) {
        return null;
      }
      hit.addAndGet(1);//get from metadata store
      boolean isDir=fields.get("isDir")!=null && fields.get("isDir").equals
              ("true");
      FileStatus status;
      if(isDir){
        status=makeDirStatus(username,path);
      }else {
        String strLen = fields.get("fs_len");
        long fs_len = strLen == null ? 0 : Long.parseLong(strLen);
        String strModifyTime = fields.get("fs_modify_time");
        long modify_time = null == strModifyTime ? 0 : Long.parseLong(strModifyTime);
        String strBlockSize = fields.get("block_size");
        long block_size = null == strBlockSize ? 0 : Long.parseLong(strBlockSize);
        status = new FileStatus(fs_len, false, 1, block_size, modify_time, 0,
                null,
                username, username, path);
      }
      return new PathMetadata(status);
    }finally {
      if (null!=redis){
        redis.close();
      }
    }
  }

  private void pathMetadataToHash(PathMetadata meta) {
    FileStatus status=meta.getFileStatus();

    Jedis redis=null;

    String key = status.getPath().toString();


    try {
      redis = jedisPool.getResource();

      if (status.isDirectory()){
        redis.hset(key,"isDir","true");
      }else {
        long fs_len = status.getLen();
        long fs_modify_time = status.getModificationTime();
        long block_size = status.getBlockSize();
        redis.hset(key, "fs_len", Long.toString(fs_len));
        redis.hset(key, "fs_modify_time", Long.toString(fs_modify_time));
        redis.hset(key, "block_size", Long.toString(block_size));
      }
      redis.hset(key,"is_delete",meta.isDeleted()?"true":"false");
    }finally {
      if(null!=redis){
        redis.close();
      }
    }
  }

  /**
   * Make a FileStatus object for a directory at given path.  The FileStatus
   * only contains what S3A needs, and omits mod time since S3A uses its own
   * implementation which returns current system time.
   *
   * @param owner username of owner
   * @param path  path to dir
   * @return new FileStatus
   */
  private FileStatus makeDirStatus(String owner, Path path) {
    return new FileStatus(0, true, 1, 0, 0, 0, null,
            owner, null, path);
  }


  @Override
  public DirListingMetadata listChildren(Path path) throws IOException {
    return null;
    /*Jedis redis=null;
    try {
      redis=jedisPool.getResource();

      String cursor=ScanParams.SCAN_POINTER_START;
      String strPath=path.toString();
      String pattern=strPath;
      if(!strPath.endsWith("/")) {
        pattern+="/";
      }
      ScanParams param = new ScanParams().match( pattern+ "*");

      final List<PathMetadata> metas = new ArrayList<>();
      do {
        ScanResult<String> res= redis.scan(cursor, param);
        for(String key:res.getResult()){
          Path child=new Path(key);
          PathMetadata metadata=hashToPathMetadata(child,username);
          metas.add(metadata);
        }
        cursor=res.getStringCursor();
      }while(!cursor.equals(ScanParams.SCAN_POINTER_START));

      return (metas.isEmpty() && get(path) == null)
              ? null
              : new DirListingMetadata(path, metas, false);
    }finally {
      if(null!=redis){
        redis.close();
      }
    }*/
  }

  @Override
  public void move(Collection<Path> pathsToDelete, Collection<PathMetadata> pathsToCreate) throws IOException {
    Preconditions.checkNotNull(pathsToDelete, "pathsToDelete is null");
    Preconditions.checkNotNull(pathsToCreate, "pathsToCreate is null");
    Preconditions.checkArgument(pathsToDelete.size() == pathsToCreate.size(),
            "Must supply same number of paths to delete/create.");

    // I feel dirty for using reentrant lock. :-|
    synchronized (this) {

      // 1. Delete pathsToDelete
      for (Path meta : pathsToDelete) {
        LOG.debug("move: deleting metadata {}", meta);
        delete(meta);
      }

      // 2. Create new destination path metadata
      for (PathMetadata meta : pathsToCreate) {
        LOG.debug("move: adding metadata {}", meta);
        put(meta);
      }

      // 3. We now know full contents of all dirs in destination subtree
      for (PathMetadata meta : pathsToCreate) {
        FileStatus status = meta.getFileStatus();
        if (status == null || status.isDirectory()) {
          continue;
        }
        DirListingMetadata dir = listChildren(status.getPath());
        if (dir != null) {  // could be evicted already
          dir.setAuthoritative(true);
        }
      }
    }
  }

  @Override
  public void put(PathMetadata meta) throws IOException {
    pathMetadataToHash(meta);
  }


  @Override
  public void put(Collection<PathMetadata> metas) throws IOException {
    for (PathMetadata metadata : metas) {
      put(metadata);
    }
  }

  @Override
  public void put(DirListingMetadata meta) throws IOException {

    LOG.debug("Saving  : {}", meta);

    // directory path
    PathMetadata p = new PathMetadata(makeDirStatus(username,meta.getPath()),
            meta.isEmpty(), false);

    // First add any missing ancestors...
    final Collection<PathMetadata> metasToPut = fullPathsToPut(p);

    // next add all children of the directory
    metasToPut.addAll(meta.getListing());

    try {
      for(PathMetadata metadata:metasToPut){
        put(metadata);
      }
    } catch (AmazonClientException e) {
      throw translateException("put", (String) null, e);
    }
  }

  /**
   * Validates a path meta-data object.
   */
  private static void checkPathMetadata(PathMetadata meta) {
    Preconditions.checkNotNull(meta);
    Preconditions.checkNotNull(meta.getFileStatus());
    Preconditions.checkNotNull(meta.getFileStatus().getPath());
  }

  /**
   * Helper method to get full path of ancestors that are nonexistent in table.
   */
  private Collection<PathMetadata> fullPathsToPut(PathMetadata meta)
          throws IOException {
    checkPathMetadata(meta);
    final Collection<PathMetadata> metasToPut = new ArrayList<>();
    // root path is not persisted
    if (!meta.getFileStatus().getPath().isRoot()) {
      metasToPut.add(meta);
    }

    // put all its ancestors if not present; as an optimization we return at its
    // first existent ancestor
    Path path = meta.getFileStatus().getPath().getParent();
    while (path != null && !path.isRoot()) {
      Map<String,String> fields=jedisPool.getResource().hgetAll(path.toString());
      if (null==fields||fields.size()==0) {
        final FileStatus status = makeDirStatus(username, path);
        metasToPut.add(new PathMetadata(status, Tristate.FALSE, false));
        path = path.getParent();
      } else {
        break;
      }
    }
    return metasToPut;
  }

  @Override
  public void prune(long modTime) throws IOException, UnsupportedOperationException {
    Jedis redis=null;
    try{
      redis=jedisPool.getResource();
      String cursor= ScanParams.SCAN_POINTER_START;
      ScanParams param = new ScanParams().match("*");

      do {
        ScanResult<String> res= redis.scan(cursor, param);
        for(String key:res.getResult()){
          String strModTime=redis.hget(key,"fs_modify_time");
          if(strModTime==null || strModTime.isEmpty()){
            return;
          }
          if (Long.parseLong(strModTime)<modTime){
            redis.hdel(key);
          }
        }
        cursor=res.getStringCursor();
      }while(!cursor.equals(ScanParams.SCAN_POINTER_START));

    }finally {
      if(null!=redis){
        redis.close();
      }
    }
  }

  @Override
  public Map<String, String> getDiagnostics() throws IOException {
    Map<String, String> map = new HashMap<>();
    map.put("name", "Hdcs metadata store backend");
    map.put("host", redisHost);
    map.put("description", "new metadata store to support the entire open " +
            "source solution");
    return map;
  }

  @Override
  public void updateParameters(Map<String, String> parameters) throws IOException {

  }

  @Override
  public synchronized void close() throws IOException {
    LOG.info("hdcs meta store count,total: {}, hit: {} ",total,hit);
    //jedisPool.close();
  }

  @Override
  public void destroy() throws IOException {
    jedisPool.getResource().flushAll();
    //jedisPool.destroy();
  }

  public void doDelete(Path path, boolean recursive, boolean tombstone) {
    Jedis redis=null;
    try {
      redis = jedisPool.getResource();
      if (tombstone) {
        pathMetadataToHash(tombstone(path));
      } else {
        redis.hdel(path.toString());
      }
    }finally {
      if(null!=redis){
        redis.close();
      }
    }

    if (recursive) {
      String cursor=ScanParams.SCAN_POINTER_START;
      String strPath=path.toString();
      String pattern=strPath;
      if(!strPath.endsWith("/")) {
        pattern+="/";
      }
      ScanParams param = new ScanParams().match( pattern+ "*");

      do {
        ScanResult<String> res= redis.scan(cursor, param);
        for(String key:res.getResult()){
          redis.hdel(key);
        }
        cursor=res.getStringCursor();
      }while(!cursor.equals(ScanParams.SCAN_POINTER_START));
    }
  }

  /**
   * Create a tombstone from the current time.
   * @param path path to tombstone
   * @return the entry.
   */
  public static PathMetadata tombstone(Path path) {
    long now = System.currentTimeMillis();
    FileStatus status = new FileStatus(0, false, 0, 0, now, path);
    return new PathMetadata(status, Tristate.UNKNOWN, true);
  }

  /**
   * Determine if directory is empty.
   * Call with lock held.
   *
   * @param p a Path, already filtered through standardize()
   * @return TRUE / FALSE if known empty / not-empty, UNKNOWN otherwise.
   */
  private Tristate isEmptyDirectory(Path p) {
    String key = PathUtil.pathToKey(p);
    byte[] bytes = jedisPool.getResource().get(key.getBytes());
    DirListingMetadata dirMeta = (DirListingMetadata) RedisUtil.deserialize
            (bytes);
    return dirMeta.withoutTombstones().isEmpty();
  }

  private boolean expired(FileStatus status, long expiry) {
    // Note: S3 doesn't track modification time on directories, so for
    // consistency with the DynamoDB implementation we ignore that here
    return status.getModificationTime() < expiry && !status.isDirectory();
  }


  /**
   * Validates a path object; it must be absolute, and contain a host
   * (bucket) component.
   */
  private Path checkPath(Path path) {
    Preconditions.checkNotNull(path);
    Preconditions.checkArgument(path.isAbsolute(), "Path %s is not absolute",
            path);
    URI uri = path.toUri();
    Preconditions.checkNotNull(uri.getScheme(), "Path %s missing scheme", path);
    Preconditions.checkArgument(uri.getScheme().equals(Constants.FS_S3A),
            "Path %s scheme must be %s", path, Constants.FS_S3A);
    Preconditions.checkArgument(!StringUtils.isEmpty(uri.getHost()), "Path %s" +
            " is missing bucket.", path);
    return path;
  }

  static class PathUtil {
    public static String pathToKey(Path path) {
      /*if (!path.isAbsolute()) {
        path = new Path(workingDir, path);
      }*/

      if (path.toUri().getScheme() != null && path.toUri().getPath().isEmpty()) {
        return "";
      }

      return path.toUri().getPath().substring(1);
    }

    /**
     * Convert a path back to a key.
     *
     * @param key input key
     * @return the path from this key
     */
    public static Path keyToPath(String key) {
      return new Path("/" + key);
    }

    /**
     * @return true iff 'ancestor' is ancestor dir in path 'f'.
     * All paths here are absolute.  Dir does not count as its own ancestor.
     */
    public static boolean isAncestorOf(Path ancestor, Path f) {
      String aStr = ancestor.toString();
      if (!ancestor.isRoot()) {
        aStr += "/";
      }
      String fStr = f.toString();
      return (fStr.startsWith(aStr));
    }

  }

  static class RedisUtil {
    public static byte[] serialize(Object object) {
      if (null == object) {
        return null;
      }

      ObjectOutput out = null;
      ByteArrayOutputStream bos = null;
      byte[] target = null;
      try {
        bos = new ByteArrayOutputStream();
        out = new ObjectOutputStream(bos);
        out.writeObject(object);
        out.flush();
        target = bos.toByteArray();
      } catch (Exception e) {
        LOG.error(e.getMessage());
      } finally {
        close(bos);
        try {
          out.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }

      return target;
    }

    public static Object deserialize(byte[] source) {
      ByteArrayInputStream bais = null;
      ObjectInputStream ois = null;
      try {
        bais = new ByteArrayInputStream(source);
        ois = new ObjectInputStream(bais);
        return ois.readObject();
      } catch (Exception e) {

      } finally {
        close(bais);
        close(ois);
      }

      return null;

    }

    public static void close(Closeable closeable) {
      if (null != closeable) {
        try {
          closeable.close();
        } catch (Exception e) {

        }
      }

    }
  }


}
