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
package org.apache.hadoop.yarn.server.sharedcachemanager.security;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.authorize.PolicyProvider;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.yarn.api.ClientSCMProtocolPB;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.SCMAdminProtocolPB;
import org.apache.hadoop.yarn.server.api.SCMUploaderProtocolPB;


/**
 * {@link PolicyProvider} for shared cache manager protocols.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class SCMPolicyProvider extends PolicyProvider {

  private static SCMPolicyProvider scmPolicyProvider =
      new SCMPolicyProvider();

  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static SCMPolicyProvider getInstance() {
    return scmPolicyProvider;
  }

  private static final Service[] SCM_SERVICES = new Service[] {
      new Service(YarnConfiguration.
        YARN_SECURITY_SERVICE_AUTHORIZATION_SHAREDCACHEMANAGER_CLIENT_PROTOCOL,
        ClientSCMProtocolPB.class),
      new Service(YarnConfiguration.
        YARN_SECURITY_SERVICE_AUTHORIZATION_SHAREDCACHEMANAGER_ADMIN_PROTOCOL,
        SCMAdminProtocolPB.class),
      new Service(YarnConfiguration.
        YARN_SECURITY_SERVICE_AUTHORIZATION_SHAREDCACHEMANAGER_UPLOADER_PROTOCOL,
        SCMUploaderProtocolPB.class),
  };

  @Override
  public Service[] getServices() {
    return copyServiceArray(SCM_SERVICES);
  }

  private Service[] copyServiceArray(Service[] services) {
    Service[] res = new Service[services.length];
    for (int i = 0; i < res.length; ++i) {
      res[i] = new Service(
          services[i].getServiceKey(), services[i].getProtocol());
    }
    return res;
  }
}
