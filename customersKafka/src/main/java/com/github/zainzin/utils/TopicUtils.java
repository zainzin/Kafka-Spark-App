package com.github.zainzin.utils;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

import java.util.Properties;

public class TopicUtils {
    public static void createTopic(String topic, int noOfPartitions, int noOfReplication) {
        ZkClient zkClient = null;
        ZkUtils zkUtils;
        try {
            String zookeeperHosts = "localhost:2181";
            int sessionTimeOutInMs = 15 * 1000;
            int connectionTimeOutInMs = 10 * 1000;

            zkClient = new ZkClient(zookeeperHosts, sessionTimeOutInMs, connectionTimeOutInMs, ZKStringSerializer$.MODULE$);
            zkUtils = new ZkUtils(zkClient, new ZkConnection(zookeeperHosts), false);

            Properties topicConfiguration = new Properties();

            AdminUtils.createTopic(zkUtils, topic, noOfPartitions, noOfReplication, topicConfiguration, null);

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (zkClient != null) {
                zkClient.close();
            }
        }
    }
}
