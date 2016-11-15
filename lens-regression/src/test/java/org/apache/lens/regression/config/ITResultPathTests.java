/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lens.regression.config;


import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;

import javax.ws.rs.client.WebTarget;
import javax.xml.bind.JAXBException;

import org.apache.lens.api.query.*;
import org.apache.lens.regression.core.constants.QueryInventory;
import org.apache.lens.regression.core.helpers.ServiceManagerHelper;
import org.apache.lens.regression.core.testHelper.BaseTestClass;
import org.apache.lens.regression.util.Util;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.util.LensUtil;

import org.testng.Assert;
import org.testng.annotations.*;

import com.jcraft.jsch.JSchException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ITResultPathTests extends BaseTestClass{

  WebTarget servLens;
  String sessionHandleString;

  private static String resultDir     = "/user/lens/lensreports";
  private static String hiveResultDir = "user/lens/lensreports-hive";
  private static String jdbcResultDir = "user/lens/lensreports-jdbc";

  private String lensSitePath = lens.getServerDir() + "/conf/lens-site.xml";
  private String hiveDriverSitePath = lens.getServerDir() + "/conf/drivers/hive/hive1/hivedriver-site.xml";
  private String jdbcDriverSitePath = lens.getServerDir() + "/conf/drivers/jdbc/jdbc1/jdbcdriver-site.xml";


  @BeforeClass(alwaysRun = true)
  public void initialize() throws IOException, JSchException, JAXBException, LensException {
    servLens = ServiceManagerHelper.init();
  }


  @BeforeMethod(alwaysRun = true)
  public void setUp(Method method) throws Exception {
    log.info("Test Name: " + method.getName());
    sessionHandleString = sHelper.openSession(lens.getCurrentDB());
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_SET, "true");
    sHelper.setAndValidateParam(LensConfConstants.QUERY_PERSISTENT_RESULT_INDRIVER, "true");
  }

  @AfterMethod(alwaysRun=true)
  public void restoreConfig() throws JSchException, IOException, JAXBException, LensException{
    qHelper.killQuery(null, "QUEUED", "all");
    qHelper.killQuery(null, "RUNNING", "all");
    if (sessionHandleString != null) {
      sHelper.closeSession();
    }
  }


  @Test(enabled=true, dataProvider="path_provider")
  public void resultPathInServerOnly() throws Exception {

    try {
      HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.RESULT_SET_PARENT_DIR, resultDir);
      Util.changeConfig(map, lensSitePath);
      lens.restart();

      QueryHandle queryHandle = (QueryHandle) qHelper.executeQuery(QueryInventory.HIVE_CUBE_QUERY).getData();
      LensQuery lensQuery = qHelper.waitForCompletion(queryHandle);
      Assert.assertEquals(lensQuery.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");
      Assert.assertEquals(lensQuery.getResultSetPath(), lens.getServerHdfsUrl() + resultDir);

    } finally {
      Util.changeConfig(lensSitePath);
      lens.restart();
    }
  }

  @Test(enabled=true, dataProvider="path_provider")
  public void resultPathInServerNDriver() throws Exception {

    try {
      HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.RESULT_SET_PARENT_DIR, resultDir);
      Util.changeConfig(map, lensSitePath);

      HashMap<String, String> map1 = LensUtil.getHashMap(LensConfConstants.RESULT_SET_PARENT_DIR, hiveResultDir);
      Util.changeConfig(map, hiveDriverSitePath);

      lens.restart();

      QueryHandle q1 = (QueryHandle) qHelper.executeQuery(QueryInventory.HIVE_CUBE_QUERY).getData();
      LensQuery lq1 = qHelper.waitForCompletion(q1);
      Assert.assertEquals(lq1.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");
      Assert.assertEquals(lq1.getResultSetPath(), lens.getServerHdfsUrl() + hiveResultDir);

      QueryHandle q2 = (QueryHandle) qHelper.executeQuery(QueryInventory.JDBC_CUBE_QUERY).getData();
      LensQuery lq2 = qHelper.waitForCompletion(q2);
      Assert.assertEquals(lq2.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");
      Assert.assertEquals(lq2.getResultSetPath(), lens.getServerHdfsUrl() + resultDir);

    } finally {
      Util.changeConfig(lensSitePath);
      Util.changeConfig(hiveDriverSitePath);
      lens.restart();
    }
  }


  @Test(enabled=true, dataProvider="path_provider")
  public void resultPathInServerNAllDrivers() throws Exception {

    try {
      HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.RESULT_SET_PARENT_DIR, resultDir);
      Util.changeConfig(map, lensSitePath);

      map = LensUtil.getHashMap(LensConfConstants.RESULT_SET_PARENT_DIR, hiveResultDir);
      Util.changeConfig(map, hiveDriverSitePath);

      map = LensUtil.getHashMap(LensConfConstants.RESULT_SET_PARENT_DIR, jdbcResultDir);
      Util.changeConfig(map, jdbcDriverSitePath);

      lens.restart();

      QueryHandle q1 = (QueryHandle) qHelper.executeQuery(QueryInventory.HIVE_CUBE_QUERY).getData();
      LensQuery lq1 = qHelper.waitForCompletion(q1);
      Assert.assertEquals(lq1.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");
      Assert.assertEquals(lq1.getResultSetPath(), lens.getServerHdfsUrl() + hiveResultDir);

      QueryHandle q2 = (QueryHandle) qHelper.executeQuery(QueryInventory.JDBC_CUBE_QUERY).getData();
      LensQuery lq2 = qHelper.waitForCompletion(q2);
      Assert.assertEquals(lq2.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");
      Assert.assertEquals(lq2.getResultSetPath(), lens.getServerHdfsUrl() + jdbcResultDir);

    } finally {
      Util.changeConfig(lensSitePath);
      Util.changeConfig(hiveDriverSitePath);
      Util.changeConfig(jdbcDriverSitePath);
      lens.restart();
    }
  }


  @Test(enabled=true, dataProvider="path_provider")
  public void resultPathInServerNSession() throws Exception {

    try {
      HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.RESULT_SET_PARENT_DIR, resultDir);
      Util.changeConfig(map, lensSitePath);
      lens.restart();

      String userResultDir = lens.getServerHdfsUrl() + "/tmp/lensreports-session";
      sHelper.setAndValidateParam(LensConfConstants.RESULT_SET_PARENT_DIR, userResultDir);

      QueryHandle q1 = (QueryHandle) qHelper.executeQuery(QueryInventory.HIVE_CUBE_QUERY).getData();
      LensQuery lq1 = qHelper.waitForCompletion(q1);
      Assert.assertEquals(lq1.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL, "Query did not succeed");
      Assert.assertEquals(lq1.getResultSetPath(), lens.getServerHdfsUrl() + userResultDir);

    } finally {
      Util.changeConfig(lensSitePath);
      lens.restart();
    }
  }

}



