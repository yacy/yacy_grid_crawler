/**
 *  Crawler
 *  Copyright 25.04.2017 by Michael Peter Christen, @0rb1t3r
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *  
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *  
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program in the file lgpl21.txt
 *  If not, see <http://www.gnu.org/licenses/>.
 */

package net.yacy.grid.crawler;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import javax.servlet.Servlet;

import org.eclipse.jetty.util.ConcurrentHashSet;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import ai.susi.mind.SusiAction;
import ai.susi.mind.SusiThought;
import net.yacy.grid.QueueName;
import net.yacy.grid.Services;
import net.yacy.grid.YaCyServices;
import net.yacy.grid.crawler.api.CrawlStartService;
import net.yacy.grid.crawler.api.CrawlerDefaultValuesService;
import net.yacy.grid.http.ObjectAPIHandler;
import net.yacy.grid.io.assets.Asset;
import net.yacy.grid.io.index.WebMapping;
import net.yacy.grid.io.messages.ShardingMethod;
import net.yacy.grid.mcp.AbstractBrokerListener;
import net.yacy.grid.mcp.BrokerListener;
import net.yacy.grid.mcp.Data;
import net.yacy.grid.mcp.MCP;
import net.yacy.grid.mcp.Service;
import net.yacy.grid.tools.DateParser;
import net.yacy.grid.tools.JSONList;
import net.yacy.grid.tools.MultiProtocolURL;

public class Crawler {

    private final static YaCyServices CRAWLER_SERVICE = YaCyServices.crawler;
    private final static String DATA_PATH = "data";
 
    // define services
    @SuppressWarnings("unchecked")
    public final static Class<? extends Servlet>[] CRAWLER_SERVICES = new Class[]{
            CrawlerDefaultValuesService.class,
            CrawlStartService.class
    };

    private final static String[] FIELDS_IN_GRAPH = new String[]{
            WebMapping.inboundlinks_sxt.name(),
            WebMapping.outboundlinks_sxt.name(),
            WebMapping.images_sxt.name(),
            WebMapping.frames_sxt.name(),
            WebMapping.iframes_sxt.name()
    };
    
    private final static Map<String, Set<MultiProtocolURL>> doubles = new ConcurrentHashMap<>();
    /**
     * broker listener, takes process messages from the queue "crawler", "webcrawler"
     * i.e. test with:
     * curl -X POST -F "message=@job.json" -F "serviceName=crawler" -F "queueName=webcrawler" http://yacygrid.com:8100/yacy/grid/mcp/messages/send.json
     * where job.json is:
{
  "metadata": {
    "process": "yacy_grid_loader",
    "count": 1
  },
  "data": [{
    "id": "201705042045000-xyz",
    "crawlingMode": "url",
    "crawlingURL": "http://yacy.net",
    "sitemapURL": "",
    "crawlingFile": "",
    "crawlingDepth": 3,
    "crawlingDepthExtension": "",
    "range": "domain",
    "mustmatch": ".*",
    "mustnotmatch": "",
    "ipMustmatch": ".*",
    "ipMustnotmatch": "",
    "indexmustmatch": ".*",
    "indexmustnotmatch": "",
    "deleteold": "off",
    "deleteIfOlderNumber": 0,
    "deleteIfOlderUnit": "day",
    "recrawl": "nodoubles",
    "reloadIfOlderNumber": 0,
    "reloadIfOlderUnit": "day",
    "crawlingDomMaxCheck": "off",
    "crawlingDomMaxPages": 1000,
    "crawlingQ": "off",
    "directDocByURL": "off",
    "storeHTCache": "off",
    "cachePolicy": "if fresh",
    "indexText": "on",
    "indexMedia": "off",
    "xsstopw": "off",
    "collection": "user",
    "agentName": "yacybot (yacy.net; crawler from yacygrid.com)",
    "user": "anonymous@nowhere.com",
    "client": "yacygrid.com"
  }],
  "actions": [{
    "type": "crawler",
    "queue": "webcrawler",
    "id": "201705042045000-xyz",
    "depth": 1,
    "sourcegraph": "test3/yacy.net.graph.json"
  }]
}

{
  "metadata": {
    "process": "yacy_grid_parser",
    "count": 1
  },
  "data": [{"collection": "test"}],
  "actions": [{
    "type": "loader",
    "queue": "webloader",
    "urls": ["http://yacy.net"],
    "collection": "test",
    "targetasset": "test3/yacy.net.warc.gz",
    "actions": [{
      "type": "parser",
      "queue": "yacyparser",
      "sourceasset": "test3/yacy.net.warc.gz",
      "targetasset": "test3/yacy.net.jsonlist",
      "targetgraph": "test3/yacy.net.graph.json"
      "actions": [{
        "type": "indexer",
        "queue": "elasticsearch",
        "sourceasset": "test3/yacy.net.jsonlist"
      },{
        "type": "crawler",
        "queue": "webcrawler",
        "sourceasset": "test3/yacy.net.graph.json"
      },
      ]
    }]
  }]
}
     */
    

    public static class CrawlerListener extends AbstractBrokerListener implements BrokerListener {

        public CrawlerListener(YaCyServices service) {
            super(service, Runtime.getRuntime().availableProcessors());
        }

        @Override
        public boolean processAction(SusiAction crawlaction, JSONArray data) {
            String id = crawlaction.getStringAttr("id");
            if (id == null || id.length() == 0) {
                Data.logger.info("Fail: Action does not have an id: " + crawlaction.toString());
                return false;
            }
            JSONObject crawl = SusiThought.selectData(data, "id", id);
            if (crawl == null) {
                Data.logger.info("Fail: ID of Action not found in data: " + crawlaction.toString());
                return false;
            }

            JSONArray urlArray = crawl.getJSONArray("crawlingURLs");
            int depth = crawlaction.getIntAttr("depth");
            if (depth == 0) {
                // this is a crawl start
                // construct the loading, parsing, indexing action
                // we take the start url from the data object
                //CrawlstartURLs crawlstartURLs = new CrawlstartURLs(crawl.getString("crawlingURL"));
                //JSONArray urlArray = crawlstartURLs.getURLs();
                String hashKey =  "";
                try {
                    hashKey = new MultiProtocolURL(urlArray.getString(0)).getHost();
                } catch (MalformedURLException | JSONException e1) {}

                // first, we must load the page(s): construct a loader message
                SusiThought json = new SusiThought();
                json.setData(data);
                Date timestamp = new Date();

                // put a loader message on the queue
                try {
                    JSONObject loaderAction = newLoaderAction(id, urlArray, 0, timestamp, 0); // action includes whole hierarchy of follow-up actions
                    json.addAction(new SusiAction(loaderAction));
                    byte[] b = json.toString(2).getBytes(StandardCharsets.UTF_8);
                    QueueName queueName = Data.gridBroker.queueName(YaCyServices.loader, YaCyServices.loader.getQueues(), ShardingMethod.LOOKUP, hashKey);
                    Data.gridBroker.send(YaCyServices.loader, queueName, b);
                } catch (IOException e) {
                    Data.logger.warn("error when starting crawl with depth = 0 with id " + id, e);
                }
            } else {
                // this is a follow-up

                // check depth
                int crawlingDepth = crawl.getInt("crawlingDepth");
                if (depth > crawlingDepth) {
                    // this is a leaf in the crawl tree (it does not mean that the crawl is finished)
                    Data.logger.info("Leaf: reached a crawl leaf for crawl " + id);
                    return true;
                }

                // load graph
                String sourcegraph = crawlaction.getStringAttr("sourcegraph");
                if (sourcegraph == null || sourcegraph.length() == 0) {
                    Data.logger.info("Fail: sourcegraph of Action is empty: " + crawlaction.toString());
                    return false;
                }
                try {
                    JSONList jsonlist = null;
                    if (crawlaction.hasAsset(sourcegraph)) {
                        jsonlist = crawlaction.getJSONListAsset(sourcegraph);
                    }
                    if (jsonlist == null) try {
                        Asset<byte[]> graphasset = Data.gridStorage.load(sourcegraph); // this must be a list of json, containing document links
                        byte[] graphassetbytes = graphasset.getPayload();
                        jsonlist = new JSONList(new ByteArrayInputStream(graphassetbytes));
                    } catch (IOException e) {
                        Data.logger.warn("could not read asset from storage: " + sourcegraph, e);
                        return false;
                    }
                    graphloop: for (int line = 0; line < jsonlist.length(); line++) {
                        JSONObject json = jsonlist.get(line);
                        if (json.has("index")) continue graphloop; // this is an elasticsearch index directive, we just skip that

                        Set<MultiProtocolURL> graph = new HashSet<>();
                        String graphurl = json.has(WebMapping.canonical_s.name()) ? json.getString(WebMapping.canonical_s.name()) : null;
                        if (graphurl != null) try {
                            graph.add(new MultiProtocolURL(graphurl));
                        } catch (MalformedURLException e) {
                            Data.logger.warn("error when starting crawl with canonical url " + graphurl, e);
                        }
                        for (String field: FIELDS_IN_GRAPH) {
                            if (json.has(field)) {
                                JSONArray a = json.getJSONArray(field);
                                urlloop: for (int i = 0; i < a.length(); i++) {
                                    String u = a.getString(i);
                                    try {
                                        graph.add(new MultiProtocolURL(u));
                                    } catch (MalformedURLException e) {
                                        Data.logger.warn("for crawl url array " + urlArray.toString() + " we discovered a bad follow-up url: " + u, e);
                                        continue urlloop;
                                    }
                                }
                            }
                        }

                        // sort out doubles and apply filters
                        List<String> nextList = new ArrayList<>();
                        String mustmatchs = crawl.getString("mustmatch");
                        Pattern mustmatch = Pattern.compile(mustmatchs);
                        String mustnotmatchs = crawl.getString("mustnotmatch");
                        Pattern mustnotmatch = Pattern.compile(mustnotmatchs);
                        if (!doubles.containsKey(id)) doubles.put(id, new ConcurrentHashSet<>());
                        final Set<MultiProtocolURL> doubleset = doubles.get(id);
                        graph.forEach(url -> {
                            if (!doubleset.contains(url)) {
                                doubleset.add(url);

                                // check if the url shall be loaded using the constraints
                                String u = url.toNormalform(true);
                                if (mustmatch.matcher(u).matches() &&
                                        !mustnotmatch.matcher(u).matches()) {
                                    // add url to next stack
                                    nextList.add(u);
                                }
                            }
                        });

                        // create partitions
                        List<JSONArray> partitions = new ArrayList<>();
                        int maxURLsPerPartition = 2;
                        nextList.forEach(url -> {
                            int c = partitions.size();
                            if (c == 0 || partitions.get(c - 1).length() >= maxURLsPerPartition) {
                                partitions.add(new JSONArray());
                                c++;
                            }
                            partitions.get(c - 1).put(url);
                        });


                        // create follow-up crawl to next depth
                        Date timestamp = new Date();
                        for (int pc = 0; pc < partitions.size(); pc++) {
                            JSONObject loaderAction = newLoaderAction(id, partitions.get(pc), depth, timestamp, pc); // action includes whole hierarchy of follow-up actions
                            SusiThought nextjson = new SusiThought()
                                    .setData(data)
                                    .addAction(new SusiAction(loaderAction));

                            // put a loader message on the queue
                            String message = nextjson.toString(2);
                            byte[] b = message.getBytes(StandardCharsets.UTF_8);
                            try {
                                Services serviceName = YaCyServices.valueOf(loaderAction.getString("type"));
                                QueueName queueName = new QueueName(loaderAction.getString("queue"));
                                Data.gridBroker.send(serviceName, queueName, b);
                                json.put(ObjectAPIHandler.SUCCESS_KEY, true);
                            } catch (IOException e) {
                                Data.logger.warn("error when starting crawl with message " + message, e);
                                json.put(ObjectAPIHandler.SUCCESS_KEY, false);
                                json.put(ObjectAPIHandler.COMMENT_KEY, e.getMessage());
                            }
                        };
                    }

                    Data.logger.info("processed message from queue and loaded graph " + sourcegraph);
                    return true;
                } catch (Throwable e) {
                    Data.logger.info("Fail: loading of sourcegraph failed: " + e.getMessage() + "\n" + crawlaction.toString(), e);
                    return false;
                }
            } // else depth != 0

            return false;
        }
    }

    private final static String PATTERN_TIMEF = "MMddHHmmssSSS"; 
    public final static SimpleDateFormat FORMAT_TIMEF = new SimpleDateFormat(PATTERN_TIMEF, Locale.US);
    
    public static JSONObject newLoaderAction(
            String id,
            JSONArray urls,
            int depth,
            Date timestamp,
            int partition) throws IOException {
        String namestub = id + "/d" + intf(depth) + "-t" + FORMAT_TIMEF.format(timestamp) + "-p" + intf(partition);
        String warcasset =  namestub + ".warc.gz";
        String webasset =  namestub + ".web.jsonlist";
        String graphasset =  namestub + ".graph.jsonlist";
        String hashKey = new MultiProtocolURL(urls.getString(0)).getHost();

        QueueName loaderQueueName = Data.gridBroker.queueName(YaCyServices.loader, YaCyServices.loader.getQueues(), ShardingMethod.LOOKUP, hashKey);
        QueueName parserQueueName = Data.gridBroker.queueName(YaCyServices.parser, YaCyServices.parser.getQueues(), ShardingMethod.LOOKUP, hashKey);
        QueueName indexerQueueName = Data.gridBroker.queueName(YaCyServices.indexer, YaCyServices.indexer.getQueues(), ShardingMethod.LOOKUP, hashKey);
        JSONObject loaderAction = new JSONObject(true)
            .put("type", YaCyServices.loader.name())
            .put("queue", loaderQueueName.name())
            .put("id", id)
            .put("urls", urls)
            .put("targetasset", warcasset)
            .put("actions", new JSONArray().put(new JSONObject(true)
                .put("type", YaCyServices.parser.name())
                .put("queue", parserQueueName.name())
                .put("id", id)
                .put("sourceasset", warcasset)
                .put("targetasset", webasset)
                .put("targetgraph", graphasset)
                .put("actions", new JSONArray().put(new JSONObject(true)
                    .put("type", YaCyServices.indexer.name())
                    .put("queue", indexerQueueName.name())
                    .put("id", id)
                    .put("sourceasset", webasset)
                 ).put(newCrawlerAction(id, depth + 1, hashKey)
                    .put("sourcegraph", graphasset)
                 ))));
        return loaderAction;
    }
    
    private final static String intf(int i) {
       String s = Integer.toString(i);
       while (s.length() < 3) s = '0' + s;
       return s;
    }

    public static JSONObject newCrawlerAction(String id, int depth, String hashKey) throws IOException {
        QueueName crawlerQueueName = Data.gridBroker.queueName(YaCyServices.crawler, YaCyServices.crawler.getQueues(), ShardingMethod.LOOKUP, hashKey);
        JSONObject crawlerAction = new JSONObject(true)
            .put("type", YaCyServices.crawler.name())
            .put("queue", crawlerQueueName.name())
            .put("id", id)
            .put("depth", depth);
        return crawlerAction;
    }
    
    /*
    public static class CrawlstartURLs {
        
        JSONArray crawlingURLArray;
        String id = "";
        String hashKey = "";
        
        public CrawlstartURLs(String crawlingURLsString) {
            crawlingURLsString = crawlingURLsString.replaceAll("%0D%0A", "\n").replaceAll("%0A", "\n").replaceAll("%0D", "\n").replaceAll(" ", "\n");
            String[] crawlingURLs = crawlingURLsString.split("\n");
            this.crawlingURLArray = new JSONArray();
            this.id = "";
            int c = 0;
            for (String u: crawlingURLs) {
                try {
                    MultiProtocolURL url = new MultiProtocolURL(u);
                    this.crawlingURLArray.put(url.toNormalform(true));
                    this.id = this.id + url.getHost() + "-";
                    if (c++ == 0) this.hashKey = url.getHost();
                } catch (MalformedURLException e) {
                    e.printStackTrace();
                }
            }
            if (id.length() > 80) id = id.substring(0, 80) + "-" + id.hashCode();
            id = id + "-" + DateParser.secondDateFormat.format(new Date()).replace(':', '-').replace(' ', '-');
        }
        
        public String getId() {
            return this.id;
        }
        
        public JSONArray getURLs() {
            return this.crawlingURLArray;
        }
        
        public String getHashKey() {
            return this.hashKey;
        }
    }
    */
    
    public static class CrawlstartURLSplitter {
        
        private List<MultiProtocolURL> crawlingURLArray;
        private List<String> badURLStrings;
        
        public CrawlstartURLSplitter(String crawlingURLsString) {
            Data.logger.info("splitting url list: " + crawlingURLsString);
            crawlingURLsString = crawlingURLsString.replaceAll("\\|http", "\nhttp").replaceAll("%7Chttp", "\nhttp").replaceAll("%0D%0A", "\n").replaceAll("%0A", "\n").replaceAll("%0D", "\n").replaceAll(" ", "\n");
            String[] crawlingURLs = crawlingURLsString.split("\n");
            this.crawlingURLArray = new ArrayList<>();
            this.badURLStrings = new ArrayList<>();
            for (String u: crawlingURLs) {
                try {
                    MultiProtocolURL url = new MultiProtocolURL(u);
                    Data.logger.info("splitted url: " + url.toNormalform(true));
                    this.crawlingURLArray.add(url);
                } catch (MalformedURLException e) {
                    this.badURLStrings.add(u);
                    Data.logger.warn("error when starting crawl with splitter url " + u + "; splitted from " + crawlingURLsString, e);
                }
            }
        }
        
        public List<MultiProtocolURL> getURLs() {
            return this.crawlingURLArray;
        }
        
        public List<String> getBadURLs() {
            return this.badURLStrings;
        }
    }
    
    public static String getCrawlID(MultiProtocolURL url, Date date) {
        String id = url.getHost();
        if (id.length() > 80) id = id.substring(0, 80) + "-" + id.hashCode();
        id = id + "-" + DateParser.secondDateFormat.format(date).replace(':', '-').replace(' ', '-');
        return id;
    }
    
    public static void main(String[] args) {
        // initialize environment variables
        List<Class<? extends Servlet>> services = new ArrayList<>();
        services.addAll(Arrays.asList(MCP.MCP_SERVICES));
        services.addAll(Arrays.asList(CRAWLER_SERVICES));
        Service.initEnvironment(CRAWLER_SERVICE, services, DATA_PATH);

        // start listener
        BrokerListener brokerListener = new CrawlerListener(CRAWLER_SERVICE);
        new Thread(brokerListener).start();

        // start server
        Service.runService(null);
        brokerListener.terminate();
    }
    
}
