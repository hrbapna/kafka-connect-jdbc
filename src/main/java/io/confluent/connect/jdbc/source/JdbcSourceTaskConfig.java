/**
 * Copyright 2015 Confluent Inc.
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
 **/

package io.confluent.connect.jdbc.source;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Map;

/**
 * Configuration options for a single JdbcSourceTask. These are processed after all
 * Connector-level configs have been parsed.
 */
public class JdbcSourceTaskConfig extends JdbcSourceConnectorConfig {

  private static final Logger log = LoggerFactory.getLogger(JdbcSourceTaskConfig.class);

  public static final String TABLES_CONFIG = "tables";
  private static final String TABLES_DOC = "List of tables for this task to watch for changes.";

  static ConfigDef config = baseConfigDef()
      .define(TABLES_CONFIG, Type.LIST, Importance.HIGH, TABLES_DOC);

  public JdbcSourceTaskConfig(Map<String, String> props) {
    super(modifyConfig(config,props), props);
  }

  private static void setupKeyStore() {
    try {
      KeyGenerator kg = KeyGenerator.getInstance("AES");
      kg.init(128);
      SecretKey sk = kg.generateKey();

      // Storing AES Secret key in keystore
      KeyStore ks = KeyStore.getInstance("JCEKS");
      char[] password = "RedL0ck!Anon".toCharArray();
      java.io.FileInputStream fis = null;

      ks.load(null, password);
      KeyStore.ProtectionParameter protParam =
              new KeyStore.PasswordProtection(password);

      KeyStore.SecretKeyEntry skEntry = new KeyStore.SecretKeyEntry(sk);
      ks.setEntry("anonymizeSKey", skEntry, protParam);

      // store away the keystore
      java.io.FileOutputStream fos = null;
      try {
        fos = new java.io.FileOutputStream("ANON_KEYSTORE_FILE");
        ks.store(fos, password);
      } finally {
        if (fos != null) {
          fos.close();
        }
      }
    }
    catch (Exception e) {
      log.info("Exception in keystore setup");
      e.printStackTrace();
    }
  }

  private static ConfigDef modifyConfig(ConfigDef config,Map<String,String >  props){
    setupKeyStore();

    for(String prop:props.keySet()){
      if((prop.contains(INCREMENTING_COLUMN_NAME_CONFIG) && !prop.startsWith(INCREMENTING_COLUMN_NAME_CONFIG))){
        config.define(prop, Type.STRING,"",Importance.MEDIUM, "Documentation",MODE_GROUP,2, ConfigDef.Width.MEDIUM,"");
      }
      else if((prop.contains(TIMESTAMP_COLUMN_NAME_CONFIG) && !prop.startsWith(TIMESTAMP_COLUMN_NAME_CONFIG)) ){
        config.define(prop, Type.STRING,"",Importance.MEDIUM, "Documentation",MODE_GROUP,3, ConfigDef.Width.MEDIUM,"");
      }
      else if(prop.contains(MODE_CONFIG) && !prop.startsWith(MODE_CONFIG)){
        config.define(prop, Type.STRING, MODE_UNSPECIFIED, ConfigDef.ValidString.in(MODE_UNSPECIFIED, MODE_BULK, MODE_TIMESTAMP, MODE_INCREMENTING, MODE_TIMESTAMP_INCREMENTING),
                Importance.HIGH, "Doc", MODE_GROUP, 1, ConfigDef.Width.MEDIUM, "", Arrays.asList(INCREMENTING_COLUMN_NAME_CONFIG, TIMESTAMP_COLUMN_NAME_CONFIG, VALIDATE_NON_NULL_CONFIG));
      }
      else if ((prop.equals(ANONYMIZE+".default")) || (prop.contains(ANONYMIZE + ".column.name"))) {
        config.define(prop,Type.STRING,"",Importance.MEDIUM, "Documentation",MODE_GROUP,3, ConfigDef.Width.MEDIUM,"");
      }
    }
    return config;
  }
}
