/**
 * Copyright 2014 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.minicluster;

import com.google.common.base.Preconditions;
import java.io.File;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.minikdc.MiniKdc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An KDC minicluster service implementation.
 */
public class KdcService implements Service {

  private static final Logger logger = LoggerFactory
      .getLogger(KdcService.class);

  /**
   * Service registration for MiniCluster factory
   */
  static {
    MiniCluster.registerService(KdcService.class);
  }

  /**
   * Service configuration keys
   */

  /**
   * Configuration settings
   */
  private Configuration hadoopConf;
  private String bindIP = "127.0.0.1";
  private String workDir;

  /**
   * Embedded KDC cluster
   */
  private Properties kdcConf;
  private MiniKdc kdc;
 

  public KdcService() {
  }

  @Override
  public void configure(ServiceConfig serviceConfig) {
    workDir = serviceConfig.get(MiniCluster.WORK_DIR_KEY);

    if (serviceConfig.contains(MiniCluster.BIND_IP_KEY)) {
      bindIP = serviceConfig.get(MiniCluster.BIND_IP_KEY);
    }

    hadoopConf = serviceConfig.getHadoopConf();
  }

  @Override
  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  @Override
  public void start() throws IOException, InterruptedException {
    Preconditions.checkState(workDir != null,
        "Working directory must be set before starting the mini KDC cluster");

    kdcConf = MiniKdc.createConf();
    if (bindIP != null) {
      kdcConf.setProperty(MiniKdc.KDC_BIND_ADDRESS, bindIP);
    }
    try {
      kdc = new MiniKdc(kdcConf, getKdcLocation(workDir));
      kdc.start();
    } catch (Exception ex) {
      throw new RuntimeException("Unexpected exception: " + ex.getMessage(), ex);
    }

    getKeytabsLocation(workDir).mkdirs();

    logger.info("KDC Minicluster Service Started.");
  }

  @Override
  public void stop() throws IOException {
    if (kdc != null) {
      kdc.stop();
      kdc = null;
    }
  }

  @Override
  public List<Class<? extends Service>> dependencies() {
    // no dependencies
    return null;
  }

  /**
   * Get the location on the local FS where we store the KDC data.
   * 
   * @param baseFsLocation
   *          The base location on the local filesystem we have write access to
   *          create dirs.
   * @return The location for KDC data.
   */
  private static File getKdcLocation(String baseFsLocation) {
    return new File(baseFsLocation, "kdc");
  }
  
  /**
   * Get the location on the local FS where we store keytabs.
   * 
   * @param baseFsLocation
   *          The base location on the local filesystem we have write access to
   *          create dirs.
   * @return The location for keytabs.
   */
  private static File getKeytabsLocation(String baseFsLocation) {
    return new File(baseFsLocation, "keytabs");
  }

  public String createKeytab(String keytabFileName, String... shortNames) throws Exception {
    File keytab = new File(getKeytabsLocation(workDir), keytabFileName);
    kdc.createPrincipal(keytab, shortNames);

    return keytab.getCanonicalPath();
  }

  public String getPrincipal(String shortName) {
    return shortName + "@" + kdc.getRealm();
  }

}
