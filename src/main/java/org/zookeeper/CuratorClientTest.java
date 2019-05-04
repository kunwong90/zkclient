package org.zookeeper;

import java.util.List;

public class CuratorClientTest {

    public static void main(String[] args) {
        CuratorClient curatorClient = new CuratorClient("localhost:2181");
        curatorClient.createParentsIfNeeded("/tt/yy/rr/ttf", true);
        /*String test1 = "/test11";
        curatorClient.create(test1, false);

        curatorClient.delete(test1);

        test1 = "/test1/test2/test3/test4";
        curatorClient.create(test1, "hello", false);

        List<String> children = curatorClient.getChildren("/11/22");
        System.out.println(children);

        curatorClient.nodeListener(curatorClient.getClient(), "/test1/test2");

        curatorClient.createPersistent("/test1/test2", "11");

        System.out.println(curatorClient.getContent(test1));

        String path = "/com/test/org/services";
        curatorClient.create(path, false);
        curatorClient.nodeListener(curatorClient.getClient(), "/com/test/org");
        //curatorClient.doDelete(path);
        curatorClient.create(path, false);
        curatorClient.createPersistent("/com/test/org", "org");
        curatorClient.create(path, "hello", false);

        curatorClient.childNodeListener(curatorClient.getClient(), "/com/test/org");
        curatorClient.create(path, "helloword232", false);*/



        try {
            Thread.sleep(30000);
        } catch (Exception e) {

        }
    }
}
