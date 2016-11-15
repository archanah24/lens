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

package org.apache.lens.regression.util;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.ws.rs.client.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.xml.bind.*;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.*;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.lens.api.APIResult;
import org.apache.lens.api.StringList;
import org.apache.lens.api.jaxb.LensJAXBContext;
import org.apache.lens.api.metastore.ObjectFactory;
import org.apache.lens.api.metastore.XProperties;
import org.apache.lens.api.metastore.XProperty;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClients;


import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.jcraft.jsch.*;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Util {

  private static final String PROPERTY_FILE = "lens.properties";
  private static String localFilePath = "target/";
  private static String localFile;
  private static String backupFile;


  private Util() {

  }

  public static Properties getPropertiesObj(String filename) {
    try {
      Properties properties = new Properties();
      log.info("filename: {}", filename);
      InputStream confStream = Util.class.getResourceAsStream("/" + filename);
      properties.load(confStream);
      confStream.close();
      return properties;
    } catch (IOException e) {
      log.info("Error in getProperies:", e);
    }
    return null;
  }

  public static String getProperty(String property) {
    Properties prop = getPropertiesObj(PROPERTY_FILE);
    return prop.getProperty(property);
  }

  public static String runRemoteCommand(String command, String url) throws Exception {

    String bodyData = "{ \"cmd\":\"" + command + "\" }";
    Client client = ClientBuilder.newClient(new ClientConfig().register(MultiPartFeature.class));
    WebTarget webTarget = client.target(url).path("run");
    Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_XML);
    Response response = invocationBuilder.post(Entity.entity(bodyData, MediaType.APPLICATION_JSON));
    String responseStr = response.readEntity(String.class);
    JSONObject jsonObj = new JSONObject(responseStr);
    log.info(jsonObj.toString());

    if (!jsonObj.get("code").toString().equals("0")) {
      log.info("******* Executing command failed");
      throw new RuntimeException("CMD : " + command + "\nCommand execution failed\n ");
    }

    return jsonObj.get("stdout").toString();
  }


  public static String runRemoteCommand(String command) throws Exception {

    if (getProperty("remote.ssh.implementation") != null) {
      return runRemoteCommand(command, getProperty("remote.ssh-service.url"));
    }

    StringBuilder outputBuffer = new StringBuilder();
    StringBuilder print = new StringBuilder();

    String userName = getProperty("lens.remote.username");
    String host = getProperty("lens.remote.host");
    String password = getProperty("lens.remote.password");

    log.info("Running command : {} on host : {} with user as {}", command, host, userName);

    JSch jsch = new JSch();
    Session session = jsch.getSession(userName, host, 22);
    session.setPassword(password);

    Properties config = new Properties();
    config.put("StrictHostKeyChecking", "no");

    session.setServerAliveInterval(10000);
    session.setConfig(config);
    session.connect(0);

    ChannelExec channel = (ChannelExec) session.openChannel("exec");
    InputStream commandOutput = channel.getInputStream();

    channel.setCommand(command);
    channel.connect();
    int readByte = commandOutput.read();
    char toAppend = ' ';
    while (readByte != 0xffffffff) {
      toAppend = (char) readByte;
      outputBuffer.append(toAppend);
      readByte = commandOutput.read();
      if (toAppend == '\n') {
        log.info(print.toString());
        print = new StringBuilder();
      } else {
        print.append(toAppend);
      }
    }
    channel.disconnect();
    session.disconnect();
    return outputBuffer.toString();
  }

  public static void writeFile(String fileName, String str) {
    try {
      PrintWriter out = new PrintWriter(fileName, "UTF-8");
      out.println(str);
      out.close();
    } catch (IOException e) {
      log.info("File Exception : ", e);
    }
  }

  public static APIResult getApiResult(Response response) {
    APIResult result = response.readEntity(APIResult.class);
    return result;
  }

  @SuppressWarnings("unchecked")
  public static <T> Object extractObject(String queryString, Class<T> c) throws IllegalAccessException {
    JAXBContext jaxbContext = null;
    Unmarshaller unmarshaller = null;
    StringReader reader = new StringReader(queryString);
    try {
      jaxbContext = new LensJAXBContext(ObjectFactory.class) {
      };
      unmarshaller = jaxbContext.createUnmarshaller();
      return (T) ((JAXBElement<?>) unmarshaller.unmarshal(reader)).getValue();
    } catch (JAXBException e) {
      System.out.println("Exception : " + e);
      return null;
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> Object getObject(String queryString, Class<T> c) throws IllegalAccessException {
    JAXBContext jaxbContext = null;
    Unmarshaller unmarshaller = null;
    StringReader reader = new StringReader(queryString);
    try {
      jaxbContext = new LensJAXBContext(c);
      unmarshaller = jaxbContext.createUnmarshaller();
      return (T) unmarshaller.unmarshal(reader);
    } catch (JAXBException e) {
      System.out.println("Exception : " + e);
      return null;
    }
  }

  @SuppressWarnings("unchecked")
  public static <T> String convertObjectToXml(T object, Class<T> clazz, String functionName)
    throws SecurityException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {

    JAXBElement<T> root = null;
    StringWriter stringWriter = new StringWriter();
    ObjectFactory methodObject = new ObjectFactory();

    Method method = methodObject.getClass().getMethod(functionName, clazz);
    root = (JAXBElement<T>) method.invoke(methodObject, object);
    try {
      getMarshaller(clazz).marshal(root, stringWriter);
    } catch (JAXBException e) {
      log.error("Not able to marshall", e);
    }
    return stringWriter.toString();
  }

  public static Marshaller getMarshaller(Class clazz) {
    JAXBContext jaxbContext = null;
    try {
      jaxbContext = new LensJAXBContext(clazz);
      Marshaller jaxbMarshaller = jaxbContext.createMarshaller();
      jaxbMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

      return jaxbMarshaller;
    } catch (JAXBException e) {
      log.error("Error : ", e);
    }

    return null;
  }

  public static HashMap<String, String> stringListToMap(StringList stringList) {
    HashMap<String, String> map = new HashMap<String, String>();
    if (stringList == null) {
      return null;
    }
    List<String> list = stringList.getElements();
    for (String element : list) {
      StringTokenizer tk = new StringTokenizer(element, "=");
      map.put(tk.nextToken(), tk.nextToken());
    }
    return map;
  }

  public static HashMap<String, String> stringListToMap(String paramList) throws Exception {
    StringList stringList = (StringList) getObject(paramList, StringList.class);
    return stringListToMap(stringList);
  }

  public static void changeConfig(HashMap<String, String> map, String remotePath, String backupFile) throws Exception {

    if (getProperty("remote.ssh.implementation") != null) {
      changeConfig(map, remotePath, backupFile, getProperty("remote.ssh-service.url"));
      return;
    }

    Path p = Paths.get(remotePath);
    String fileName = p.getFileName().toString();
    if (backupFile == null) {
      backupFile = localFilePath + "backup-" + fileName;
    } else {
      backupFile = localFilePath + backupFile;
    }

    localFile = localFilePath + fileName;
    log.info("Copying " + remotePath + " to " + localFile);
    remoteFile("get", remotePath, localFile);
    remoteFile("get", remotePath, localFile);
    Files.copy(new File(localFile).toPath(), new File(backupFile).toPath(), REPLACE_EXISTING);

    DocumentBuilder docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    Document doc = docBuilder.parse(new FileInputStream(localFile));
    doc.normalize();

    NodeList rootNodes = doc.getElementsByTagName("configuration");
    Node root = rootNodes.item(0);
    Element rootElement = (Element) root;
    NodeList property = rootElement.getElementsByTagName("property");

    for (int i = 0; i < property.getLength(); i++) {  //Deleting redundant properties from the document
      Node prop = property.item(i);
      Element propElement = (Element) prop;
      Node propChild = propElement.getElementsByTagName("name").item(0);

      Element nameElement = (Element) propChild;
      if (map.containsKey(nameElement.getTextContent())) {
        rootElement.removeChild(prop);
        i--;
      }
    }

    Iterator<Entry<String, String>> ab = map.entrySet().iterator();
    while (ab.hasNext()) {
      Entry<String, String> entry = ab.next();
      String propertyName = entry.getKey();
      String propertyValue = entry.getValue();
      System.out.println(propertyName + " " + propertyValue + "\n");
      Node newNode = doc.createElement("property");
      rootElement.appendChild(newNode);
      Node newName = doc.createElement("name");
      Element newNodeElement = (Element) newNode;

      newName.setTextContent(propertyName);
      newNodeElement.appendChild(newName);

      Node newValue = doc.createElement("value");
      newValue.setTextContent(propertyValue);
      newNodeElement.appendChild(newValue);
    }
    prettyPrint(doc);
    remoteFile("put", remotePath, localFile);
  }

  public static void changeConfig(HashMap<String, String> map, String remotePath) throws Exception {
    changeConfig(map, remotePath, null);
  }

  public static void changeConfig(HashMap<String, String> map, String remotePath, String backupFile, String url)
    throws Exception {

    Path p = Paths.get(remotePath);
    String fileName = p.getFileName().toString();
    if (backupFile == null) {
      backupFile = localFilePath + "backup-" + fileName;
    } else {
      backupFile = localFilePath + backupFile;
    }

    localFile = localFilePath + fileName;

    Client client = ClientBuilder.newClient(new ClientConfig().register(MultiPartFeature.class));
    WebTarget webTarget = client.target(url + "download?file=" + remotePath);
    Invocation.Builder invocationBuilder = webTarget.request(MediaType.APPLICATION_XML);
    Response response = invocationBuilder.get();
    String filecontent = response.readEntity(String.class);

    writeFile(backupFile, filecontent);
    Files.copy(new File(backupFile).toPath(), new File(localFile).toPath(), REPLACE_EXISTING);

    DocumentBuilder docBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    Document doc = docBuilder.parse(new FileInputStream(localFile));
    doc.normalize();

    NodeList rootNodes = doc.getElementsByTagName("configuration");
    Node root = rootNodes.item(0);
    Element rootElement = (Element) root;
    NodeList property = rootElement.getElementsByTagName("property");

    for (int i = 0; i < property.getLength(); i++) {  //Deleting redundant properties from the document
      Node prop = property.item(i);
      Element propElement = (Element) prop;
      Node propChild = propElement.getElementsByTagName("name").item(0);

      Element nameElement = (Element) propChild;
      if (map.containsKey(nameElement.getTextContent())) {
        rootElement.removeChild(prop);
        i--;
      }
    }

    Iterator<Entry<String, String>> ab = map.entrySet().iterator();
    while (ab.hasNext()) {
      Entry<String, String> entry = ab.next();
      String propertyName = entry.getKey();
      String propertyValue = entry.getValue();
      System.out.println(propertyName + " " + propertyValue + "\n");
      Node newNode = doc.createElement("property");
      rootElement.appendChild(newNode);
      Node newName = doc.createElement("name");
      Element newNodeElement = (Element) newNode;

      newName.setTextContent(propertyName);
      newNodeElement.appendChild(newName);

      Node newValue = doc.createElement("value");
      newValue.setTextContent(propertyValue);
      newNodeElement.appendChild(newValue);
    }
    prettyPrint(doc);
    remoteCopyFile(url+"upload", localFile, remotePath);
  }

  /*
   * function to save the changes in the document
   */
  public static final void prettyPrint(Document xml) throws TransformerFactoryConfigurationError, TransformerException {
    xml.normalize();
    Transformer transformer = TransformerFactory.newInstance().newTransformer();
    transformer.setOutputProperty(OutputKeys.INDENT, "yes");
    transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
    StreamResult output = new StreamResult(new File(localFile));
    Source input = new DOMSource(xml);
    transformer.transform(input, output);
  }

  /*
   * function to download or upload a file to a remote server
   */
  public static void remoteFile(String function, String remotePath, String localPath) throws JSchException,
      SftpException {

    String serverUrl = getProperty("lens.remote.host");
    String serverUname = getProperty("lens.remote.username");
    String serverPass = getProperty("lens.remote.password");

    JSch jsch = new JSch();
    Session session = jsch.getSession(serverUname, serverUrl);

    UserInfo ui = null;
    session.setUserInfo(ui);
    session.setPassword(serverPass);

    Properties config = new Properties();
    config.put("StrictHostKeyChecking", "no");
    session.setConfig(config);
    session.connect();

    Channel channel = session.openChannel("sftp");
    channel.connect();

    ChannelSftp sftpChannel = (ChannelSftp) channel;
    if (function.equals("get")) {
      sftpChannel.get(remotePath, localPath);
    } else if (function.equals("put")) {
      sftpChannel.put(localPath, remotePath);
    }
    sftpChannel.exit();
    session.disconnect();

  }

  public static void changeConfig(String remotePath, String backupFile) throws JSchException, SftpException,
      IOException {

    String fileName = Paths.get(remotePath).getFileName().toString();
    if (backupFile == null){
      backupFile = localFilePath + "backup-" + fileName;
    }else{
      backupFile = localFilePath + backupFile;
    }
    log.info("Copying " + backupFile + " to " + remotePath);

    if (getProperty("remote.ssh.implementation") != null) {
      remoteCopyFile(getProperty("remote.ssh-service.url") + "upload", backupFile, remotePath);
      return;
    }

    remoteFile("put", remotePath, backupFile);
  }

  public static void changeConfig(String remotePath) throws JSchException, SftpException, IOException {
    changeConfig(remotePath, null);
  }

  public static Map<String, String> mapFromXProperties(XProperties xProperties) {
    Map<String, String> properties = new HashMap<String, String>();
    if (xProperties != null && xProperties.getProperty() != null
        && !xProperties.getProperty().isEmpty()) {
      for (XProperty xp : xProperties.getProperty()) {
        properties.put(xp.getName(), xp.getValue());
      }
    }
    return properties;
  }

  public static XProperties xPropertiesFromMap(Map<String, String> map) {
    ObjectFactory xCF = new ObjectFactory();
    if (map != null && !map.isEmpty()) {
      XProperties xp = xCF.createXProperties();
      List<XProperty> xpList = xp.getProperty();
      for (Map.Entry<String, String> e : map.entrySet()) {
        XProperty property = xCF.createXProperty();
        property.setName(e.getKey());
        property.setValue(e.getValue());
        xpList.add(property);
      }

      return xp;
    }
    return null;
  }

  public static List<XProperty> xPropertyFromMap(Map<String, String> map) {
    List<XProperty> xpList = new ArrayList<XProperty>();
    if (map != null && !map.isEmpty()) {
      for (Map.Entry<String, String> e : map.entrySet()) {
        XProperty property = new XProperty();
        property.setName(e.getKey());
        property.setValue(e.getValue());
        xpList.add(property);
      }
    }
    return xpList;
  }

  public static XMLGregorianCalendar getXMLGregorianCalendar(Date d) {
    if (d == null) {
      return null;
    }

    GregorianCalendar c1 = new GregorianCalendar();
    c1.setTime(d);
    try {
      return DatatypeFactory.newInstance().newXMLGregorianCalendar(c1);
    } catch (DatatypeConfigurationException e) {
      log.info("Error converting date " + d, e);
      return null;
    }
  }

  public static String getJobIdFromProgressMsg(String progressMsg){
    Pattern jobIdPattern = Pattern.compile("(job_\\d*_\\d*)");
    Matcher matcher = jobIdPattern.matcher(progressMsg);
    String jobid=null;
    if (matcher.find()) {
      jobid = matcher.group(1);
    }
    return jobid;
  }

  public static String getMapredJobPrority(String url, String jobId) throws IOException, IllegalAccessException,
      InstantiationException, ParserConfigurationException, SAXException {

    String xmlPathExpr = "/conf/property[name='mapreduce.job.priority']/value";
    String jobConfXml = sendHttpGetRequest(url.replace("JOB_ID", jobId));
    String priority = getValueFromXml(jobConfXml, xmlPathExpr);
    return priority;
  }

  public static String sendHttpGetRequest(String url) throws IOException {
    HttpGet request = new HttpGet(url);
    request.addHeader("Accept", "application/xml");
    HttpResponse response = new DefaultHttpClient().execute(request);
    BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), "UTF-8"));
    StringBuffer result = new StringBuffer();
    String line = "";
    while ((line = rd.readLine()) != null) {
      result.append(line);
    }
    return result.toString();
  }

  public static Document getDocument(String xmlContent) throws ParserConfigurationException, SAXException, IOException {

    DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xmlContent));
    Document doc = db.parse(is);
    return doc;
  }

  public static String getValueFromXml(String xmlContent, String xmlPathExpr) {

    String value    = null;
    try {
      Document doc = getDocument(xmlContent);
      XPath xpath  = XPathFactory.newInstance().newXPath();
      value        = xpath.compile(xmlPathExpr).evaluate(doc);

    } catch (ParserConfigurationException e) {
      log.error("error", e);
    } catch (SAXException e) {
      log.error("error", e);
    } catch (IOException e) {
      log.error("error", e);
    } catch (XPathExpressionException e) {
      log.error("error", e);
    }
    return value;
  }


  public static String getCurrentDate(String format){
    SimpleDateFormat sdf = new SimpleDateFormat(format);
    return sdf.format(new Date());
  }

  public static String getFormattedDate(String epochTime, String format){
    SimpleDateFormat sdf = new SimpleDateFormat(format);
    return sdf.format(Long.parseLong(epochTime));
  }

  public static String modifyDate(String dateString, String format, int calenderIntervalUnit, int diff)
    throws ParseException {

    SimpleDateFormat sdf = new SimpleDateFormat(format);
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(sdf.parse(dateString));
    calendar.add(calenderIntervalUnit, diff);
    return sdf.format(calendar.getTime());
  }

  public static XMLGregorianCalendar getGregorianCalendar(String date, String format) throws ParseException,
      DatatypeConfigurationException {
    GregorianCalendar cal = new GregorianCalendar();
    cal.setTime(new SimpleDateFormat(format).parse(date));
    cal.setTimeZone(TimeZone.getTimeZone("UTC"));
    XMLGregorianCalendar calendar = DatatypeFactory.newInstance().newXMLGregorianCalendar(cal);
    return calendar;
  }

  public static XMLGregorianCalendar getGregorianCalendar(String date) throws ParseException,
      DatatypeConfigurationException {
    return getGregorianCalendar(date, "yyyy-MM-dd HH:mm:ss");  //"yyyy-MM-dd'T'HH:mm:ss.SSSX"
  }

  public static String getDateStringFromGregorainCalender(XMLGregorianCalendar xcal, String format){
    Date dt = xcal.toGregorianCalendar().getTime();
    SimpleDateFormat sdf = new SimpleDateFormat(format);
    return sdf.format(dt);
  }

  public static void remoteCopyFile(String uploadUrl, String source, String destination) throws IOException {
    CloseableHttpClient httpClient = HttpClients.createDefault();
    log.info(uploadUrl);
    HttpPost uploadFile = new HttpPost(uploadUrl);
    MultipartEntityBuilder builder = MultipartEntityBuilder.create();
    File f = new File(source);
    builder.addBinaryBody(
        destination,
        f,
        ContentType.MULTIPART_FORM_DATA,
        f.getName()
    );
    HttpEntity multipart = builder.build();
    uploadFile.setEntity(multipart);
    CloseableHttpResponse response = httpClient.execute(uploadFile);
    StatusLine sl = response.getStatusLine();
    log.info("status code: "+sl.getStatusCode());
    if (sl.getStatusCode()!=200) {
      throw new RuntimeException("copy failed !!!");
    }
    log.info("copy of "+destination+" succesful");
  }

}
