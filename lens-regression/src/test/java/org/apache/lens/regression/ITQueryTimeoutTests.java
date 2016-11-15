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

package org.apache.lens.regression;

import java.lang.reflect.Method;
import java.util.HashMap;

import javax.ws.rs.client.WebTarget;

import org.apache.lens.api.LensConf;
import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.cube.parse.CubeQueryConfUtil;
import org.apache.lens.regression.core.constants.QueryInventory;
import org.apache.lens.regression.core.helpers.ServiceManagerHelper;
import org.apache.lens.regression.core.testHelper.BaseTestClass;
import org.apache.lens.regression.util.Util;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.util.LensUtil;

import org.apache.log4j.Logger;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class ITQueryTimeoutTests extends BaseTestClass {

  WebTarget servLens;
  private String sessionHandleString;
  private static Logger logger = Logger.getLogger(ITQueryTimeoutTests.class);

  String jdbcConf1 = lens.getServerDir() + "/conf/drivers/jdbc/prod/jdbcdriver-site.xml";
  String hiveconf1 = lens.getServerDir() + "/conf/drivers/hive/hive1/hivedriver-site.xml";
  String lensConf = lens.getServerDir() + "/conf/lens-site.xml";
  String longRunningJdbcQuery = QueryInventory.getQueryFromInventory("JDBC.LONG_RUNNING_QUERY");

  @BeforeClass(alwaysRun = true)
  public void initialize() throws Exception {
    servLens = ServiceManagerHelper.init();
  }

  @BeforeMethod(alwaysRun = true)
  public void setUp(Method method) throws Exception {
    logger.info("Test Name: " + method.getName());
    sessionHandleString = sHelper.openSession(lens.getCurrentDB());
    sHelper.setAndValidateParam(CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, "false");
  }

  @AfterMethod(alwaysRun = true)
  public void closeSession() throws Exception {
    logger.info("Closing Session");
    sHelper.closeSession();
  }

  /* LENS-968 : Monitor run time for queries and terminate long running queries
   */

  @Test(enabled = true)
  public void testAtServerLevelConf() throws Exception {

    try {
      HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.QUERY_TIMEOUT_MILLIS,
          "3000", LensConfConstants.QUERY_EXPIRY_INTERVAL_MILLIS, "2000");
      Util.changeConfig(map, lensConf);

      lens.restart();

      QueryHandle q1 = (QueryHandle) qHelper.executeQuery(QueryInventory.getSleepQuery("10")).getData();
      QueryHandle q2 = (QueryHandle) qHelper.executeQuery(longRunningJdbcQuery).getData();

      Thread.sleep(5000);

      LensQuery lq1 = qHelper.getLensQuery(q1);
      LensQuery lq2 = qHelper.getLensQuery(q2);

      Assert.assertEquals(lq1.getStatus().getStatus(), QueryStatus.Status.CANCELED);
      Assert.assertEquals(lq2.getStatus().getStatus(), QueryStatus.Status.CANCELED);

    } finally{
      Util.changeConfig(lensConf);
      lens.restart();
    }
  }

  @Test(enabled = true)
  public void testAtDriverLevelConf() throws Exception {

    try {
      HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.QUERY_EXPIRY_INTERVAL_MILLIS, "2000");
      Util.changeConfig(map, lensConf);

      map = LensUtil.getHashMap(LensConfConstants.QUERY_TIMEOUT_MILLIS, "5000");
      Util.changeConfig(map, jdbcConf1);
      Util.changeConfig(map, hiveconf1);

      lens.restart();

      QueryHandle q1 = (QueryHandle) qHelper.executeQuery(longRunningJdbcQuery).getData();
      QueryHandle q2 = (QueryHandle) qHelper.executeQuery(QueryInventory.getSleepQuery("10")).getData();

      Thread.sleep(10000);

      LensQuery lq1 = qHelper.getLensQuery(sessionHandleString, q1);
      LensQuery lq2 = qHelper.getLensQuery(sessionHandleString, q2);

      Assert.assertEquals(lq1.getStatus().getStatus(), QueryStatus.Status.CANCELED);
      Assert.assertEquals(lq2.getStatus().getStatus(), QueryStatus.Status.CANCELED);

    } finally {

      Util.changeConfig(lensConf);
      Util.changeConfig(jdbcConf1);
      Util.changeConfig(hiveconf1);
      lens.restart();
    }
  }

  @Test(enabled = true)
  public void bothInServerNDriverConf() throws Exception {

    try {

      HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.QUERY_TIMEOUT_MILLIS,
          "5000000", LensConfConstants.QUERY_EXPIRY_INTERVAL_MILLIS, "2000");
      Util.changeConfig(map, lensConf);

      map = LensUtil.getHashMap(LensConfConstants.QUERY_TIMEOUT_MILLIS, "3000");
      Util.changeConfig(map, jdbcConf1);
      Util.changeConfig(map, hiveconf1);

      lens.restart();

      QueryHandle q1 = (QueryHandle) qHelper.executeQuery(longRunningJdbcQuery).getData();
      QueryHandle q2 = (QueryHandle) qHelper.executeQuery(QueryInventory.getSleepQuery("10")).getData();

      Thread.sleep(5000);

      LensQuery lq1 = qHelper.getLensQuery(sessionHandleString, q1);
      LensQuery lq2 = qHelper.getLensQuery(sessionHandleString, q2);

      Assert.assertEquals(lq1.getStatus().getStatus(), QueryStatus.Status.CANCELED);
      Assert.assertEquals(lq2.getStatus().getStatus(), QueryStatus.Status.CANCELED);

    } finally {

      Util.changeConfig(lensConf);
      Util.changeConfig(jdbcConf1);
      Util.changeConfig(hiveconf1);
      lens.restart();
    }
  }

  //Query is completed
  @Test(enabled = true)
  public void testQueryCompletionBeforeTimeout() throws Exception {

    try {
      HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.QUERY_TIMEOUT_MILLIS,
          "20000", LensConfConstants.QUERY_EXPIRY_INTERVAL_MILLIS, "10000");
      Util.changeConfig(map, lensConf);

      lens.restart();

      QueryHandle q1 = (QueryHandle) qHelper.executeQuery(QueryInventory.JDBC_CUBE_QUERY).getData();
      //Wait for expiry interval time
      Thread.sleep(20000);
      LensQuery lq1 = qHelper.getLensQuery(sessionHandleString, q1);
      Assert.assertEquals(lq1.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

    } finally {
      Util.changeConfig(lensConf);
      lens.restart();
    }
  }


  @Test(enabled = true)
  public void settingOnlyAtQueryconf() throws Exception {

    LensConf lensConf = new LensConf();
    lensConf.addProperty(LensConfConstants.QUERY_TIMEOUT_MILLIS, "3000");

    QueryHandle q1 = (QueryHandle) qHelper.executeQuery(longRunningJdbcQuery, sessionHandleString, lensConf).getData();
    QueryHandle q2 = (QueryHandle) qHelper.executeQuery(QueryInventory.getSleepQuery("10"), sessionHandleString,
        lensConf).getData();

    Thread.sleep(5000);

    LensQuery lq1 = qHelper.getLensQuery(q1);
    LensQuery lq2 = qHelper.getLensQuery(q2);

    Assert.assertEquals(lq1.getStatus().getStatus(), QueryStatus.Status.CANCELED);
    Assert.assertEquals(lq2.getStatus().getStatus(), QueryStatus.Status.CANCELED);
  }

  @Test(enabled = true)
  public void settingInServerDriverNQueryConf() throws Exception {

    try {

      HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.QUERY_TIMEOUT_MILLIS,
          "5000000", LensConfConstants.QUERY_EXPIRY_INTERVAL_MILLIS, "2000");
      Util.changeConfig(map, lensConf);

      map = LensUtil.getHashMap(LensConfConstants.QUERY_TIMEOUT_MILLIS, "5000000");
      Util.changeConfig(map, jdbcConf1);
      Util.changeConfig(map, hiveconf1);

      lens.restart();

      LensConf lensConf = new LensConf();
      lensConf.addProperty(LensConfConstants.QUERY_TIMEOUT_MILLIS, "3000");

      QueryHandle q1 = (QueryHandle) qHelper.executeQuery(longRunningJdbcQuery, sessionHandleString,
          lensConf).getData();
      QueryHandle q2 = (QueryHandle) qHelper.executeQuery(QueryInventory.getSleepQuery("10"), sessionHandleString,
          lensConf).getData();

      Thread.sleep(5000);

      LensQuery lq1 = qHelper.getLensQuery(q1);
      LensQuery lq2 = qHelper.getLensQuery(q2);

      Assert.assertEquals(lq1.getStatus().getStatus(), QueryStatus.Status.CANCELED);
      Assert.assertEquals(lq2.getStatus().getStatus(), QueryStatus.Status.CANCELED);

    } finally {

      Util.changeConfig(lensConf);
      Util.changeConfig(jdbcConf1);
      Util.changeConfig(hiveconf1);
      lens.restart();
    }
  }
}
