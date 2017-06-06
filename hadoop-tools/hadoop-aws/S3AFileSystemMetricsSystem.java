package org.apache.hadoop.fs.s3a;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by yongfu on 6/2/2017.
 */
@InterfaceAudience.Private
public class S3AFileSystemMetricsSystem {
    private static final Logger LOG = LoggerFactory.getLogger(
            S3AFileSystemMetricsSystem.class);
    private static MetricsSystemImpl instance;
    private static int numFileSystems;

    //private ctor
    private S3AFileSystemMetricsSystem(){

    }

    public static synchronized void fileSystemStarted() {
        if (numFileSystems == 0) {
            instance = new MetricsSystemImpl();
            instance.init("s3a-file-system");
        }
        numFileSystems++;
    }

    public static synchronized void fileSystemClosed() {
        if (numFileSystems == 1) {
            instance.publishMetricsNow();
            instance.stop();
            instance.shutdown();
            instance = null;
        }
        numFileSystems--;
    }

    public static synchronized void registerSource(String name, String desc,
                                      MetricsSource source) {
        //caller has to use unique name to register source

        MetricsSource exist=instance.getSource(name);
        if(null==exist){
            LOG.debug(name+" is not exist,registering ...");
            instance.register(name, desc, source);
        }
    }

    public static synchronized void unregisterSource(String name) {
        if (instance != null) {
            //publish metrics before unregister a metrics source
            instance.publishMetricsNow();
            instance.unregisterSource(name);
        }
    }
}
