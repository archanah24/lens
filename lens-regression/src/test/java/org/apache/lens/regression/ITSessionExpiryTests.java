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

import java.io.IOException;
import java.lang.reflect.Method;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import javax.xml.bind.JAXBException;

import org.apache.lens.api.query.LensQuery;
import org.apache.lens.api.query.QueryHandle;
import org.apache.lens.api.query.QueryStatus;
import org.apache.lens.regression.core.constants.DriverConfig;
import org.apache.lens.regression.core.constants.QueryInventory;
import org.apache.lens.regression.core.constants.SessionURL;
import org.apache.lens.regression.core.helpers.ServiceManagerHelper;
import org.apache.lens.regression.core.testHelper.BaseTestClass;
import org.apache.lens.regression.core.type.MapBuilder;
import org.apache.lens.regression.util.AssertUtil;
import org.apache.lens.regression.util.Util;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.error.LensException;
import org.apache.lens.server.api.util.LensUtil;

import org.apache.log4j.Logger;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.jcraft.jsch.JSchException;


public class ITSessionExpiryTests extends BaseTestClass {

  private WebTarget servLens;
  private String sessionHandleString;

  private static Logger logger = Logger.getLogger(ITSessionExpiryTests.class);
  private final String lensSiteConf = lens.getServerDir() + "/conf/lens-site.xml";
  private final String hiveDriverConf = lens.getServerDir() + "/conf/drivers/hive/hive1/hivedriver-site.xml";
  String url = lens.getParam("remote.ssh-service.url");


  @BeforeClass(alwaysRun = true)
  public void initialize() throws IOException, JSchException, JAXBException, LensException {
    servLens = ServiceManagerHelper.init();
  }

  @BeforeMethod(alwaysRun = true)
  public void setUp(Method method) throws Exception {
    logger.info("Test Name: " + method.getName());
    logger.info("Creating a new Session");
    sessionHandleString = sHelper.openSession(lens.getCurrentDB());
  }

  @AfterMethod(alwaysRun = true)
  public void closeSession() {
    logger.info("Closing Session");
    try{
      sHelper.closeSession();
    }catch (Exception e){
      logger.info("Session is already deleted");
    }
  }

  public void closeSession(String session) throws LensException {
    MapBuilder query = new MapBuilder("sessionid", session);
    Response response = lens.exec("delete", SessionURL.SESSION_BASE_URL, servLens, null, query, null,
        MediaType.APPLICATION_XML, null);
  }


  @Test(enabled = true)
  public void testSessionExpiry() throws Exception {

    String sessionHandle = null;
    try {
      HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.SESSION_TIMEOUT_SECONDS, "20",
          LensConfConstants.SESSION_EXPIRY_SERVICE_INTERVAL_IN_SECS, "10");
      Util.changeConfig(map, lensSiteConf, null, url);
      lens.restart();

      sessionHandle = sHelper.openSession("user", "pass");
      // Waiting for session timeout + 10s buffer
      Thread.sleep(30000);

      MapBuilder query = new MapBuilder("sessionid", sessionHandle);
      Response response = lens.sendQuery("get", SessionURL.SESSION_LIST_RESOURCE_URL, query);
      AssertUtil.assertGone(response);

    } finally {
      if (sessionHandle != null) {
        closeSession(sessionHandle);
      }
      Util.changeConfig(lensSiteConf);
      lens.restart();
    }
  }

  @Test(enabled = true)
  public void sessionExpiryForRunningQuery() throws Exception {

    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    String sessionHandle = null;

    try {
      HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.SESSION_TIMEOUT_SECONDS, "20",
          LensConfConstants.SESSION_EXPIRY_SERVICE_INTERVAL_IN_SECS, "10");
      Util.changeConfig(map, lensSiteConf);
      lens.restart();

      sessionHandle = sHelper.openSession("user", "pass", lens.getCurrentDB());
      logger.info("Session created now : " + dateFormat.format(new Date()));
      QueryHandle q = (QueryHandle) qHelper.executeQuery(QueryInventory.getSleepQuery("50")).getData();
      logger.info("Query submittted now : " + dateFormat.format(new Date()));

      // Waiting for session timeout
      Thread.sleep(20000);
      logger.info("After wait for session timeout : " + dateFormat.format(new Date()));

      LensQuery lq = qHelper.waitForCompletion(q);
      logger.info("After query completion : " + dateFormat.format(new Date()));
      Assert.assertEquals(lq.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

      MapBuilder query = new MapBuilder("sessionid", sessionHandle);
      Response response = lens.sendQuery("get", SessionURL.SESSION_LIST_RESOURCE_URL, query);
      AssertUtil.assertGone(response);

    }finally {
      if (sessionHandle != null) {
        closeSession(sessionHandle);
      }

      Util.changeConfig(lensSiteConf);
      lens.restart();
    }
  }

  @Test(enabled = true)
  public void sessionExpiryForQueuedQuery() throws Exception {

    String session1 = null, session2  = null;
    try {
      HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.SESSION_TIMEOUT_SECONDS, "20",
          LensConfConstants.SESSION_EXPIRY_SERVICE_INTERVAL_IN_SECS, "10");
      Util.changeConfig(map, lensSiteConf);

      map = LensUtil.getHashMap(DriverConfig.MAX_CONCURRENT_QUERIES, "1");
      Util.changeConfig(map, hiveDriverConf);
      lens.restart();

      session1 = sHelper.openSession("user1", "pass1", lens.getCurrentDB());
      session2 = sHelper.openSession("user2", "pass2", lens.getCurrentDB());
      logger.info("After session creation : " + sHelper.getSessionLastAccesTime(session2));

      QueryHandle q1 = (QueryHandle) qHelper.executeQuery(QueryInventory.getSleepQuery("8"), null, session1).getData();
      QueryHandle q2 = (QueryHandle) qHelper.executeQuery(QueryInventory.getSleepQuery("2"), null, session2).getData();

      // Waiting for session timeout
      Thread.sleep(15000);
      logger.info("After wait for session timeout : " + sHelper.getSessionLastAccesTime(session2));
      sessionHandleString = sHelper.openSession(lens.getCurrentDB());

      LensQuery lq1 = qHelper.waitForCompletion(q1);
      LensQuery lq2 = qHelper.waitForCompletion(q2);

      logger.info("After query completion : " + sHelper.getSessionLastAccesTime(session2));

      Assert.assertEquals(lq1.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
      Assert.assertEquals(lq2.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

      Thread.sleep(5000);

      Response response = lens.sendQuery("get", SessionURL.SESSION_LIST_RESOURCE_URL,
          new MapBuilder("sessionid", session1));
      AssertUtil.assertGone(response);

      response = lens.sendQuery("get", SessionURL.SESSION_LIST_RESOURCE_URL,
          new MapBuilder("sessionid", session2));
      AssertUtil.assertGone(response);

    } finally {

      if (session1 != null) {
        closeSession(session1);
      }
      if (session2 != null) {
        closeSession(session2);
      }

      Util.changeConfig(hiveDriverConf);
      Util.changeConfig(lensSiteConf);
      lens.restart();
    }
  }

  @Test(enabled = true)
  public void sessionAccessTimeOnRestart() throws Exception {

    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    String sessionHandle = null;
    long origAccessTime = 0, accessTimeAfterRestart = 0;

    try {
      HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.SESSION_TIMEOUT_SECONDS, "20",
          LensConfConstants.SESSION_EXPIRY_SERVICE_INTERVAL_IN_SECS, "10");
      Util.changeConfig(map, lensSiteConf);
      lens.restart();

      sessionHandle = sHelper.openSession("r1", "p1", lens.getCurrentDB());
      logger.info("Session created now : " + dateFormat.format(new Date()));

      origAccessTime = sHelper.getSessionLastAccesTime(sessionHandle);
      Thread.sleep(10000);
      lens.restart();
      accessTimeAfterRestart = sHelper.getSessionLastAccesTime(sessionHandle);

      logger.info("orig : " + origAccessTime + "\tafter restrat : " + accessTimeAfterRestart);
      Assert.assertEquals(accessTimeAfterRestart, origAccessTime, "Access time has changed after restart");

      logger.info("After restart : " + dateFormat.format(new Date()));
      Thread.sleep(10000);

      MapBuilder query = new MapBuilder("sessionid", sessionHandle);
      Response response = lens.sendQuery("get", SessionURL.SESSION_LIST_RESOURCE_URL, query);
      AssertUtil.assertGone(response);

    }finally {
      if (sessionHandle != null) {
        closeSession(sessionHandle);
      }
      Util.changeConfig(lensSiteConf);
      lens.restart();
    }
  }

  @Test(enabled = true)
  public void testSessionAccessTimeUpdation() throws Exception {

    String session1 = null;
    try {

      HashMap<String, String> map = LensUtil.getHashMap(LensConfConstants.SESSION_TIMEOUT_SECONDS, "20",
          LensConfConstants.SESSION_EXPIRY_SERVICE_INTERVAL_IN_SECS, "10");
      Util.changeConfig(map, lensSiteConf);
      lens.restart();

      session1 = sHelper.openSession("access1", "pass1", lens.getCurrentDB());
      long sessionCreaionTime = sHelper.getSessionLastAccesTime(session1);
      logger.info("After Session creation : " + sessionCreaionTime);

      QueryHandle q1 = (QueryHandle) qHelper.executeQuery(QueryInventory.getSleepQuery("10"), null, session1).getData();
      long afterQueryFire = sHelper.getSessionLastAccesTime(session1);
      logger.info("After query launch : " + afterQueryFire);

      Assert.assertNotEquals(afterQueryFire, sessionCreaionTime, "Session access time is not updated");

      LensQuery lq1 = qHelper.getLensQuery(session1, q1);
      long afterQueryStatusCheck = sHelper.getSessionLastAccesTime(session1);
      logger.info("After status check : " + afterQueryStatusCheck);

      Assert.assertNotEquals(afterQueryStatusCheck, afterQueryFire, "Session access time is not updated");

      Response response = lens.sendQuery("get", SessionURL.SESSION_LIST_RESOURCE_URL,
          new MapBuilder("sessionid", session1));
      long aftersessionListCall = sHelper.getSessionLastAccesTime(session1);

      //Accessstime shouldn't be updated for session api call
      Assert.assertEquals(aftersessionListCall, afterQueryStatusCheck, "Session access time shouldnt be updated");

      QueryHandle q2 = (QueryHandle) qHelper.executeQuery(QueryInventory.JDBC_CUBE_QUERY, null, session1).getData();
      afterQueryFire = sHelper.getSessionLastAccesTime(session1);
      logger.info("After 2nd query : " + afterQueryFire);

      Assert.assertNotEquals(afterQueryFire, aftersessionListCall, "Session access time is not updated");

      response = lens.sendQuery("get", SessionURL.SESSION_LIST_RESOURCE_URL, new MapBuilder("sessionid", session1));
      AssertUtil.assertSucceededResponse(response);

      sessionHandleString = sHelper.openSession(lens.getCurrentDB());

      qHelper.waitForCompletion(q1);
      qHelper.waitForCompletion(q2);

      Thread.sleep(20000);

      response = lens.sendQuery("get", SessionURL.SESSION_LIST_RESOURCE_URL, new MapBuilder("sessionid", session1));
      AssertUtil.assertGone(response);


    } finally {
      if (session1 != null) {
        closeSession(session1);
      }

      Util.changeConfig(lensSiteConf);
      lens.restart();
    }
  }

}
