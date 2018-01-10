package org.apache.hadoop.fs.s3a.s3guard;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.MockS3ClientFactory;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ClientFactory;
import org.apache.hadoop.fs.s3a.Tristate;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

import static org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY;
import static org.apache.hadoop.fs.s3a.Constants.FS_S3A;
import static org.apache.hadoop.fs.s3a.Constants.S3GUARD_DDB_TABLE_CREATE_KEY;
import static org.apache.hadoop.fs.s3a.Constants.S3_CLIENT_FACTORY_IMPL;
import static org.apache.hadoop.fs.s3a.Constants.SECRET_KEY;

public class TestHdcsMetadataStore extends MetadataStoreTestBase {
  private static final Logger LOG= LoggerFactory.getLogger
          (TestHdcsMetadataStore.class);
  private static final String BUCKET = "TestHdcsMetadataStore";
  private static final String S3URI =
          URI.create(FS_S3A + "://" + BUCKET + "/").toString();

  private class HdcsMSContact extends AbstractMSContract{
    private final S3AFileSystem s3aFs;
    private HdcsMetadataStore hdcs=new HdcsMetadataStore();

    public HdcsMSContact() throws IOException {
      this(new Configuration());
    }

    HdcsMSContact(Configuration conf) throws IOException {
      conf.setClass(S3_CLIENT_FACTORY_IMPL, MockS3ClientFactory.class,
              S3ClientFactory.class);
      conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, S3URI);
      // setting config for creating a DynamoDBClient against local server
      conf.set(ACCESS_KEY, "dummy-access-key");
      conf.set(SECRET_KEY, "dummy-secret-key");
      conf.setBoolean(S3GUARD_DDB_TABLE_CREATE_KEY, true);
      conf.setClass(S3Guard.S3GUARD_DDB_CLIENT_FACTORY_IMPL,
              DynamoDBLocalClientFactory.class, DynamoDBClientFactory.class);
      conf.set("redis.host","client01");
      s3aFs=(S3AFileSystem) FileSystem.newInstance(conf);
      hdcs.initialize(s3aFs);
    }

    @Override
    public S3AFileSystem getFileSystem() throws IOException {
      return s3aFs;
    }

    @Override
    public MetadataStore getMetadataStore() throws IOException {
      return hdcs;
    }
  }

  @Override
  public AbstractMSContract createContract() throws IOException {
    return new HdcsMSContact();
  }

  @Override
  public AbstractMSContract createContract(Configuration conf) throws IOException {
    return new HdcsMSContact(conf);
  }

  @Test
  public void testSerialize(){
    Path p=new Path("s3a://test/aaa");
    S3AFileStatus status=new S3AFileStatus(Tristate.FALSE,p,"root");
    PathMetadata metadata=new PathMetadata(status);
    byte[] bytes=HdcsMetadataStore.RedisUtil.serialize(metadata);
    LOG.info("after serialized:"+new String(bytes));
  }
}
