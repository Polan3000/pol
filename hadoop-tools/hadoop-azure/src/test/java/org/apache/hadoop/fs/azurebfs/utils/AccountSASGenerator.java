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

package org.apache.hadoop.fs.azurebfs.utils;

import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.services.AbfsUriQueryBuilder;

import java.time.Instant;

/**
 * Account SAS Generator to be used by tests
 */

public class AccountSASGenerator extends SASGenerator {
  /**
   * Creates Account SAS
   * https://learn.microsoft.com/en-us/rest/api/storageservices/create-account-sas
   * @param accountKey: the storage account key
   */
  public AccountSASGenerator(byte[] accountKey) {
    super(accountKey);
  }

  public String getAccountSAS(String accountName) throws AzureBlobFileSystemException {
    // retaining only the account name
    accountName = getCanonicalAccountName(accountName);
    String sp = "racwdl";
    String sv = "2021-06-08";
    String srt = "sco";

    String st = ISO_8601_FORMATTER.format(Instant.now().minus(FIVE_MINUTES));
    String se = ISO_8601_FORMATTER.format(Instant.now().plus(ONE_DAY));

    String ss = "bf";
    String spr = "https";
    String signature = computeSignatureForSAS(sp, ss, srt, st, se, sv, accountName);

    AbfsUriQueryBuilder qb = new AbfsUriQueryBuilder();
    qb.addQuery("sp", sp);
    qb.addQuery("ss", ss);
    qb.addQuery("srt", srt);
    qb.addQuery("st", st);
    qb.addQuery("se", se);
    qb.addQuery("sv", sv);
    qb.addQuery("sig", signature);
    return qb.toString().substring(1);
  }

  private String computeSignatureForSAS(String signedPerm, String signedService, String signedResType,
      String signedStart, String signedExp, String signedVersion, String accountName) {

    StringBuilder sb = new StringBuilder();
    sb.append(accountName);
    sb.append("\n");
    sb.append(signedPerm);
    sb.append("\n");
    sb.append(signedService);
    sb.append("\n");
    sb.append(signedResType);
    sb.append("\n");
    sb.append(signedStart);
    sb.append("\n");
    sb.append(signedExp);
    sb.append("\n");
    sb.append("\n"); // signedIP
    sb.append("\n"); // signedProtocol
    sb.append(signedVersion);
    sb.append("\n");
    sb.append("\n"); //signed encryption scope

    String stringToSign = sb.toString();
    LOG.debug("Account SAS stringToSign: " + stringToSign.replace("\n", "."));
    return computeHmac256(stringToSign);
  }
}