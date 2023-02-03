package io.confluent.translation.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class KafkaUtils {


    private static Properties primarySiteconfigs = new Properties();


    static {
        try {
            try (InputStream is = KafkaUtils.class.getResourceAsStream("/ccloud.properties")) {
                primarySiteconfigs.load(is);
            }
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }



    }

    public static Properties getPrimarySiteConfigs() {
        return primarySiteconfigs;
    }


}
