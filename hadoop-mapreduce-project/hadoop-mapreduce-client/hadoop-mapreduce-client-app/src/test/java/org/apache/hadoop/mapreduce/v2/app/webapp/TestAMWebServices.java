/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapreduce.v2.app.webapp;

import static org.apache.hadoop.yarn.webapp.WebServicesTestUtils.assertResponseStatusCode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.util.Set;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.JettyUtils;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.MockAppContext;
import org.apache.hadoop.util.Sets;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.GuiceServletConfig;
import org.apache.hadoop.yarn.webapp.WebServicesTestUtils;

import com.google.inject.Scopes;
import com.google.inject.servlet.GuiceFilter;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.google.inject.Guice;
import com.google.inject.servlet.ServletModule;

/**
 * Test the MapReduce Application master info web services api's. Also test
 * non-existent urls.
 *
 *  /ws/v1/mapreduce
 *  /ws/v1/mapreduce/info
 */
public class TestAMWebServices extends JerseyTest {

  private static Configuration conf = new Configuration();
  private static MockAppContext appContext;

  private static class WebServletModule extends ServletModule {
    @Override
    protected void configureServlets() {

      appContext = new MockAppContext(0, 1, 1, 1);
      appContext.setBlacklistedNodes(Sets.newHashSet("badnode1", "badnode2"));

      bind(JAXBContextResolver.class);
      bind(AMWebServices.class);
      bind(GenericExceptionHandler.class);
      bind(AppContext.class).toInstance(appContext);
      bind(Configuration.class).toInstance(conf);

      bind(GuiceFilter.class).in(Scopes.SINGLETON);
    }
  }

  static {
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    GuiceServletConfig.setInjector(
        Guice.createInjector(new WebServletModule()));
  }

  public TestAMWebServices() {
  }

  @Test
  public void testAM() throws JSONException, Exception {
    WebTarget r = target();
    Response response =
        r.path("ws").path("v1").path("mapreduce").request(MediaType.APPLICATION_JSON)
            .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    verifyAMInfo(json.getJSONObject("info"), appContext);
  }

  @Test
  public void testAMSlash() throws JSONException, Exception {
    WebTarget r = target();
    Response response =
        r.path("ws").path("v1").path("mapreduce/").request(MediaType.APPLICATION_JSON)
            .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    verifyAMInfo(json.getJSONObject("info"), appContext);
  }

  @Test
  public void testAMDefault() throws JSONException, Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("mapreduce/").request().get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    verifyAMInfo(json.getJSONObject("info"), appContext);
  }

  @Test
  public void testAMXML() throws JSONException, Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("mapreduce").request(MediaType.APPLICATION_XML)
        .get(Response.class);
    assertEquals(MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String xml = response.readEntity(String.class);
    verifyAMInfoXML(xml, appContext);
  }

  @Test
  public void testInfo() throws JSONException, Exception {
    WebTarget r = target();
    Response response =
        r.path("ws").path("v1").path("mapreduce").path("info").request(MediaType.APPLICATION_JSON)
            .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    verifyAMInfo(json.getJSONObject("info"), appContext);
  }

  @Test
  public void testInfoSlash() throws JSONException, Exception {
    WebTarget r = target();
    Response response =
        r.path("ws").path("v1").path("mapreduce").path("info/").request(MediaType.APPLICATION_JSON)
            .get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    verifyAMInfo(json.getJSONObject("info"), appContext);
  }

  @Test
  public void testInfoDefault() throws JSONException, Exception {
    WebTarget r = target();
    Response response =
        r.path("ws").path("v1").path("mapreduce").path("info/").request().get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    verifyAMInfo(json.getJSONObject("info"), appContext);
  }

  @Test
  public void testInfoXML() throws JSONException, Exception {
    WebTarget r = target();
    Response response =
        r.path("ws").path("v1").path("mapreduce").path("info/").request(MediaType.APPLICATION_XML)
            .get(Response.class);
    assertEquals(MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String xml = response.readEntity(String.class);
    verifyAMInfoXML(xml, appContext);
  }

  @Test
  public void testInvalidUri() throws JSONException, Exception {
    WebTarget r = target();
    String responseStr = "";
    try {
      responseStr = r.path("ws").path("v1").path("mapreduce").path("bogus")
          .request(MediaType.APPLICATION_JSON).get(String.class);
      fail("should have thrown exception on invalid uri");
    } catch (WebApplicationException ue) {
      Response response = ue.getResponse();
      assertResponseStatusCode(Status.NOT_FOUND, response.getStatusInfo());
      WebServicesTestUtils.checkStringMatch("error string exists and shouldn't", "", responseStr);
    }
  }

  @Test
  public void testInvalidUri2() throws JSONException, Exception {
    WebTarget r = target();
    String responseStr = "";
    try {
      responseStr = r.path("ws").path("v1").path("invalid").request(MediaType.APPLICATION_JSON)
          .get(String.class);
      fail("should have thrown exception on invalid uri");
    } catch (WebApplicationException ue) {
      Response response = ue.getResponse();
      assertResponseStatusCode(Status.NOT_FOUND, response.getStatusInfo());
      WebServicesTestUtils.checkStringMatch("error string exists and shouldn't", "", responseStr);
    }
  }

  @Test
  public void testInvalidAccept() throws JSONException, Exception {
    WebTarget r = target();
    String responseStr = "";
    try {
      responseStr =
          r.path("ws").path("v1").path("mapreduce").request(MediaType.TEXT_PLAIN).get(String.class);
      fail("should have thrown exception on invalid uri");
    } catch (WebApplicationException ue) {
      Response response = ue.getResponse();
      assertResponseStatusCode(Status.INTERNAL_SERVER_ERROR, response.getStatusInfo());
      WebServicesTestUtils.checkStringMatch("error string exists and shouldn't", "", responseStr);
    }
  }

  @Test
  public void testBlacklistedNodes() throws JSONException, Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("mapreduce").path("blacklistednodes")
        .request(MediaType.APPLICATION_JSON).get(Response.class);
    assertEquals(MediaType.APPLICATION_JSON + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    JSONObject json = response.readEntity(JSONObject.class);
    assertEquals("incorrect number of elements", 1, json.length());
    verifyBlacklistedNodesInfo(json, appContext);
  }

  @Test
  public void testBlacklistedNodesXML() throws Exception {
    WebTarget r = target();
    Response response = r.path("ws").path("v1").path("mapreduce").path("blacklistednodes")
        .request(MediaType.APPLICATION_XML).get(Response.class);
    assertEquals(MediaType.APPLICATION_XML + "; " + JettyUtils.UTF_8,
        response.getMediaType().toString());
    String xml = response.readEntity(String.class);
    verifyBlacklistedNodesInfoXML(xml, appContext);
  }

  public void verifyAMInfo(JSONObject info, AppContext ctx) throws JSONException {
    assertEquals("incorrect number of elements", 5, info.length());

    verifyAMInfoGeneric(ctx, info.getString("appId"), info.getString("user"),
        info.getString("name"), info.getLong("startedOn"), info.getLong("elapsedTime"));
  }

  public void verifyAMInfoXML(String xml, AppContext ctx)
      throws JSONException, Exception {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList nodes = dom.getElementsByTagName("info");
    assertEquals("incorrect number of elements", 1, nodes.getLength());

    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);
      verifyAMInfoGeneric(ctx,
          WebServicesTestUtils.getXmlString(element, "appId"),
          WebServicesTestUtils.getXmlString(element, "user"),
          WebServicesTestUtils.getXmlString(element, "name"),
          WebServicesTestUtils.getXmlLong(element, "startedOn"),
          WebServicesTestUtils.getXmlLong(element, "elapsedTime"));
    }
  }

  public void verifyAMInfoGeneric(AppContext ctx, String id, String user,
      String name, long startedOn, long elapsedTime) {

    WebServicesTestUtils.checkStringMatch("id", ctx.getApplicationID()
        .toString(), id);
    WebServicesTestUtils.checkStringMatch("user", ctx.getUser().toString(),
        user);
    WebServicesTestUtils.checkStringMatch("name", ctx.getApplicationName(),
        name);

    assertEquals("startedOn incorrect", ctx.getStartTime(), startedOn);
    assertTrue("elapsedTime not greater then 0", (elapsedTime > 0));

  }
  
  public void verifyBlacklistedNodesInfo(JSONObject blacklist, AppContext ctx)
    throws JSONException, Exception{
    JSONArray array = blacklist.getJSONArray("blacklistedNodes");
    assertEquals(array.length(), ctx.getBlacklistedNodes().size());
    for (int i = 0; i < array.length(); i++) {
      assertTrue(ctx.getBlacklistedNodes().contains(array.getString(i)));
    }
  }
  
  public void verifyBlacklistedNodesInfoXML(String xml, AppContext ctx)
      throws JSONException, Exception {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    InputSource is = new InputSource();
    is.setCharacterStream(new StringReader(xml));
    Document dom = db.parse(is);
    NodeList infonodes = dom.getElementsByTagName("blacklistednodesinfo");
    assertEquals("incorrect number of elements", 1, infonodes.getLength());
    NodeList nodes = dom.getElementsByTagName("blacklistedNodes");
    Set<String> blacklistedNodes = ctx.getBlacklistedNodes();
    assertEquals("incorrect number of elements", blacklistedNodes.size(),
        nodes.getLength());
    for (int i = 0; i < nodes.getLength(); i++) {
      Element element = (Element) nodes.item(i);
      assertTrue(
          blacklistedNodes.contains(element.getFirstChild().getNodeValue()));
    }
  }
}
