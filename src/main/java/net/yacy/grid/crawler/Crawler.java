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
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import javax.servlet.Servlet;

import org.json.JSONArray;
import org.json.JSONObject;

import ai.susi.mind.SusiAction;
import ai.susi.mind.SusiThought;
import net.yacy.grid.Services;
import net.yacy.grid.YaCyServices;
import net.yacy.grid.crawler.api.CrawlStartService;
import net.yacy.grid.crawler.api.CrawlerDefaultValuesService;
import net.yacy.grid.io.assets.Asset;
import net.yacy.grid.io.index.WebMapping;
import net.yacy.grid.io.messages.GridQueue;
import net.yacy.grid.io.messages.ShardingMethod;
import net.yacy.grid.mcp.AbstractBrokerListener;
import net.yacy.grid.mcp.BrokerListener;
import net.yacy.grid.mcp.Data;
import net.yacy.grid.mcp.MCP;
import net.yacy.grid.mcp.Service;
import net.yacy.grid.tools.Classification.ContentDomain;
import net.yacy.grid.tools.DateParser;
import net.yacy.grid.tools.GitTool;
import net.yacy.grid.tools.JSONList;
import net.yacy.grid.tools.MultiProtocolURL;

/**
 * The Crawler main class
 * 
 * performance debugging:
 * http://localhost:8300/yacy/grid/mcp/info/threaddump.txt
 * http://localhost:8300/yacy/grid/mcp/info/threaddump.txt?count=100
 */
public class Crawler {

    private final static YaCyServices CRAWLER_SERVICE = YaCyServices.crawler;
    private final static String DATA_PATH = "data";
    public static int[] CRAWLER_PRIORITY_DIMENSIONS = YaCyServices.crawler.getQueues().length == 1 ? new int[] {1, 0} : new int[] {YaCyServices.crawler.getQueues().length - 1, 1};
    public static int[] LOADER_PRIORITY_DIMENSIONS = YaCyServices.loader.getQueues().length == 1 ? new int[] {1, 0} : new int[] {YaCyServices.loader.getQueues().length - 1, 1};
    public static int[] PARSER_PRIORITY_DIMENSIONS = YaCyServices.parser.getQueues().length == 1 ? new int[] {1, 0} : new int[] {YaCyServices.parser.getQueues().length - 1, 1};
    public static int[] INDEXER_PRIORITY_DIMENSIONS = YaCyServices.indexer.getQueues().length == 1 ? new int[] {1, 0} : new int[] {YaCyServices.indexer.getQueues().length - 1, 1};
 
    public static void initPriorityQueue(int priorityDimension) {
        CRAWLER_PRIORITY_DIMENSIONS = priorityDimensions(YaCyServices.crawler, priorityDimension);
        LOADER_PRIORITY_DIMENSIONS = priorityDimensions(YaCyServices.loader, priorityDimension);
        PARSER_PRIORITY_DIMENSIONS = priorityDimensions(YaCyServices.parser, priorityDimension);
        INDEXER_PRIORITY_DIMENSIONS = priorityDimensions(YaCyServices.indexer, priorityDimension);
    }
    
    private static int[] priorityDimensions(YaCyServices service, int d) {
        return service.getQueues().length <= d ? new int[] {service.getQueues().length, 0} : new int[] {service.getQueues().length - d, d};
    }
    
    // define services
    @SuppressWarnings("unchecked")
    public final static Class<? extends Servlet>[] CRAWLER_SERVICES = new Class[]{
            CrawlerDefaultValuesService.class,
            CrawlStartService.class
    };

    private final static String[] FIELDS_IN_GRAPH = new String[]{
            WebMapping.inboundlinks_sxt.name(),
            WebMapping.outboundlinks_sxt.name(),
            //WebMapping.images_sxt.name(),
            WebMapping.frames_sxt.name(),
            WebMapping.iframes_sxt.name()
    };
    
    private final static Map<String, DoubleCache> doubles = new ConcurrentHashMap<>();
    private static long doublesLastCleanup = System.currentTimeMillis();
    private final static long doublesCleanupTimeout = 1000L * 60L * 60L * 24L * 7L; // cleanup after 7 days
    private final static long doublesCleanupPeriod = 1000L * 60L * 10L; // do cleanup each 10 minutes
    private static class DoubleCache {
        public Set<Integer> doubleHashes;
        public long time;
        public DoubleCache() {
            this.time = System.currentTimeMillis();
            this.doubleHashes = ConcurrentHashMap.newKeySet();
        }
    }
    
    private static void doDoubleCleanup() {
        long now = System.currentTimeMillis();
        if (now - doublesLastCleanup < doublesCleanupPeriod) return;
        doublesLastCleanup = now;
        Iterator<Map.Entry<String, DoubleCache>> i = doubles.entrySet().iterator();
        while (i.hasNext()) {
            Map.Entry<String, DoubleCache> cache = i.next();
            if ((now - cache.getValue().time) > doublesCleanupTimeout) {
                cache.getValue().doubleHashes.clear();
                i.remove();
            }
        }
    }

    public static class CrawlerListener extends AbstractBrokerListener implements BrokerListener {

        private String[] blacklist_crawler_names_list, blacklist_indexer_names_list;
        private Map<String, Blacklist> blacklists_crawler, blacklists_indexer;
        
        public CrawlerListener(YaCyServices service, String[] blacklist_crawler_names_list, String[] blacklist_indexer_names_list) {
            super(service, Runtime.getRuntime().availableProcessors());
            this.blacklist_crawler_names_list = blacklist_crawler_names_list;
            this.blacklist_indexer_names_list = blacklist_indexer_names_list;
            this.blacklists_crawler = new ConcurrentHashMap<>();
            this.blacklists_indexer = new ConcurrentHashMap<>();
        }

        private final Blacklist getBlacklistCrawler(String processName, int processNumber) {
            String key = processName + "_" + processNumber;
            Blacklist blacklist = blacklists_crawler.get(key);
            if (blacklist == null) {
                this.blacklists_crawler.put(key, blacklist = loadBlacklist(this.blacklist_crawler_names_list));
            }
            return blacklist;
        }
        private final Blacklist getBlacklistIndexer(String processName, int processNumber) {
            String key = processName + "_" + processNumber;
            Blacklist blacklist = blacklists_indexer.get(key);
            if (blacklist == null) {
                this.blacklists_indexer.put(key, blacklist = loadBlacklist(this.blacklist_indexer_names_list));
            }
            return blacklist;
        }
        
        private final Blacklist loadBlacklist(String[] names) {
            Blacklist blacklist = new Blacklist();
            for (String name: names) {
                File f = new File(Data.gridServicePath, "conf/" + name.trim());
                if (!f.exists()) f = new File("conf/" + name.trim());
                if (!f.exists()) continue;
                try {
                    blacklist.load(f);
                } catch (IOException e) {
                    Data.logger.warn("", e);
                }
            }
            return blacklist;
        }

        @Override
        public boolean processAction(SusiAction crawlaction, JSONArray data, String processName, int processNumber) {
            doDoubleCleanup();
            String id = crawlaction.getStringAttr("id");
            if (id == null || id.length() == 0) {
                Data.logger.info("Crawler.processAction Fail: Action does not have an id: " + crawlaction.toString());
                return false;
            }
            JSONObject crawl = SusiThought.selectData(data, "id", id);
            if (crawl == null) {
                Data.logger.info("Crawler.processAction Fail: ID of Action not found in data: " + crawlaction.toString());
                return false;
            }

            int depth = crawlaction.getIntAttr("depth");
            int crawlingDepth = crawl.getInt("crawlingDepth");
            int priority =  crawl.has("priority") ? crawl.getInt("priority") : 0;
            // check depth (this check should be deprecated because we limit by omitting the crawl message at crawl tree leaves)
            if (depth > crawlingDepth) {
                // this is a leaf in the crawl tree (it does not mean that the crawl is finished)
                Data.logger.info("Crawler.processAction Leaf: reached a crawl leaf for crawl " + id + ", depth = " + crawlingDepth);
                return true;
            }

            // load graph
            String sourcegraph = crawlaction.getStringAttr("sourcegraph");
            if (sourcegraph == null || sourcegraph.length() == 0) {
                Data.logger.info("Crawler.processAction Fail: sourcegraph of Action is empty: " + crawlaction.toString());
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
                    Data.logger.warn("Crawler.processAction could not read asset from storage: " + sourcegraph, e);
                    return false;
                }
                
                // declare filter from the crawl profile
                String mustmatchs = crawl.getString("mustmatch");
                Pattern mustmatch = Pattern.compile(mustmatchs);
                String mustnotmatchs = crawl.getString("mustnotmatch");
                Pattern mustnotmatch = Pattern.compile(mustnotmatchs);
                // filter for indexing steering
                String indexmustmatchs = crawl.getString("indexmustmatch");
                Pattern indexmustmatch = Pattern.compile(indexmustmatchs);
                String indexmustnotmatchs = crawl.getString("indexmustnotmatch");
                Pattern indexmustnotmatch = Pattern.compile(indexmustnotmatchs);
                
                // For each of the parsed document, there is a target graph.
                // The graph contains all url elements which may appear in a document.
                // In the following loop we collect all urls which may be of interest for the next depth of the crawl.
                Set<String> nextList = new HashSet<>();
                Blacklist blacklist_crawler = getBlacklistCrawler(processName, processNumber);
                graphloop: for (int line = 0; line < jsonlist.length(); line++) {
                    JSONObject json = jsonlist.get(line);
                    if (json.has("index")) continue graphloop; // this is an elasticsearch index directive, we just skip that

                    String sourceurl = json.has(WebMapping.url_s.getSolrFieldName()) ? json.getString(WebMapping.url_s.getSolrFieldName()) : "";
                    Set<MultiProtocolURL> graph = new HashSet<>();
                    String graphurl = json.has(WebMapping.canonical_s.name()) ? json.getString(WebMapping.canonical_s.name()) : null;
                    if (graphurl != null) try {
                        graph.add(new MultiProtocolURL(graphurl));
                    } catch (MalformedURLException e) {
                        Data.logger.warn("Crawler.processAction error when starting crawl with canonical url " + graphurl, e);
                    }
                    for (String field: FIELDS_IN_GRAPH) {
                        if (json.has(field)) {
                            JSONArray a = json.getJSONArray(field);
                            urlloop: for (int i = 0; i < a.length(); i++) {
                                String u = a.getString(i);
                                try {
                                    graph.add(new MultiProtocolURL(u));
                                } catch (MalformedURLException e) {
                                    Data.logger.warn("Crawler.processAction we discovered a bad follow-up url: " + u, e);
                                    continue urlloop;
                                }
                            }
                        }
                    }

                    // sort out doubles and apply filters
                    if (!doubles.containsKey(id)) doubles.put(id, new DoubleCache());
                    final DoubleCache doublecache = doubles.get(id);
                    Data.logger.info("Crawler.processAction processing sub-graph with " + graph.size() + " urls for url " + sourceurl);
                    graph.forEach(url -> {
                        ContentDomain cd = url.getContentDomainFromExt();
                        if (cd == ContentDomain.TEXT || cd == ContentDomain.ALL) {
                            // check if the url shall be loaded using the constraints
                            String u = url.toNormalform(true);
                            if (mustmatch.matcher(u).matches() && !mustnotmatch.matcher(u).matches()) {
                                Integer urlhash = url.hashCode();
                                if (!doublecache.doubleHashes.contains(urlhash)) {
                                    doublecache.doubleHashes.add(urlhash);
                                    // finally check the blacklist
                                    Blacklist.BlacklistInfo blacklistInfo = blacklist_crawler.isBlacklisted(u, url);
                                    if (blacklistInfo == null) {
                                        // add url to next stack
                                        nextList.add(u);
                                    } else {
                                        Data.logger.info("Crawler.processAction crawler blacklist pattern '" + blacklistInfo.matcher.pattern().toString() + "' removed url '" + u + "' from crawl list " + blacklistInfo.source + ":  " + blacklistInfo.info);
                                    }
                                }
                            }
                        }
                    });
                    Data.logger.info("Crawler.processAction processed sub-graph " + ((line + 1)/2)  + "/" + jsonlist.length()/2 + " for url " + sourceurl);
                }

                // divide the nextList into two sub-lists, one which will reach the indexer and another one which will not cause indexing
                @SuppressWarnings("unchecked")
                List<String>[] indexNoIndex = new List[2];
                indexNoIndex[0] = new ArrayList<>(); // for: index
                indexNoIndex[1] = new ArrayList<>(); // for: no-Index
                Blacklist blacklist_indexer = getBlacklistIndexer(processName, processNumber);
                nextList.forEach(url -> {
                    boolean indexConstratntFromCrawlProfil = indexmustmatch.matcher(url).matches() && !indexmustnotmatch.matcher(url).matches();
                    Blacklist.BlacklistInfo blacklistInfo = blacklist_indexer.isBlacklisted(url, null);
                    boolean indexConstraintFromBlacklist = blacklistInfo == null;
                    if (indexConstratntFromCrawlProfil && indexConstraintFromBlacklist) {
                        indexNoIndex[0].add(url);
                    } else {
                        indexNoIndex[1].add(url);
                    }
                });

                long timestamp = System.currentTimeMillis();
                
                for (int ini = 0; ini < 2; ini++) {
                    // create partitions
                    List<JSONArray> partitions = createPartition(indexNoIndex[ini], 4);

                    // create follow-up crawl to next depth
                    for (int pc = 0; pc < partitions.size(); pc++) {
                        JSONObject loaderAction = newLoaderAction(priority, id, partitions.get(pc), depth, 0, timestamp + ini, pc, depth < crawlingDepth, ini == 0); // action includes whole hierarchy of follow-up actions
                        SusiThought nextjson = new SusiThought()
                                .setData(data)
                                .addAction(new SusiAction(loaderAction));

                        // put a loader message on the queue
                        String message = nextjson.toString(2);
                        byte[] b = message.getBytes(StandardCharsets.UTF_8);
                        try {
                            Services serviceName = YaCyServices.valueOf(loaderAction.getString("type"));
                            GridQueue queueName = new GridQueue(loaderAction.getString("queue"));
                            Data.gridBroker.send(serviceName, queueName, b);
                        } catch (IOException e) {
                            Data.logger.warn("error when starting crawl with message " + message, e);
                        }
                    };
                }
                Data.logger.info("Crawler.processAction processed graph with " +  jsonlist.length()/2 + " subgraphs from " + sourcegraph);
                return true;
            } catch (Throwable e) {
                Data.logger.info("Crawler.processAction Fail: loading of sourcegraph failed: " + e.getMessage() + "\n" + crawlaction.toString(), e);
                return false;
            }
        }
    }
    
    private static List<JSONArray> createPartition(Collection<String> urls, int partitionSize) {
        List<JSONArray> partitions = new ArrayList<>();
        urls.forEach(url -> {
            int c = partitions.size();
            if (c == 0 || partitions.get(c - 1).length() >= partitionSize) {
                partitions.add(new JSONArray());
                c++;
            }
            partitions.get(c - 1).put(url);
        });
        return partitions;
    }

    private final static String PATTERN_TIMEF = "MMddHHmmssSSS"; 
    
    /**
     * Create a new loader action. This action contains all follow-up actions after
     * loading to create a steeing of parser, indexing and follow-up crawler actions.
     * @param id the crawl id
     * @param urls the urls which are part of the same actions
     * @param depth the depth of the crawl step (0 is start depth)
     * @param retry the number of load re-tries (0 is no retry, shows that this is the first attempt)
     * @param timestamp the current time when the crawler created the action
     * @param partition unique number of the url set partition. This is used to create asset names.
     * @param doCrawling flag: if true, create a follow-up crawling action. set this to false to terminate crawling afterwards
     * @param doIndexing flag: if true, do an indexing after loading. set this to false if the purpose is only a follow-up crawl after parsing
     * @return the action json
     * @throws IOException
     */
    public static JSONObject newLoaderAction(
            int priority,
            String id,
            JSONArray urls,
            int depth,
            int retry,
            long timestamp,
            int partition,
            boolean doCrawling,
            boolean doIndexing) throws IOException {
        // create file names for the assets: this uses depth and partition information
        SimpleDateFormat FORMAT_TIMEF = new SimpleDateFormat(PATTERN_TIMEF, Locale.US); // we must create this here to prevent concurrency bugs which are there in the date formatter :((
        String namestub = id + "/d" + intf(depth) + "-t" + FORMAT_TIMEF.format(new Date(timestamp)) + "-p" + intf(partition);
        String warcasset =  namestub + ".warc.gz";
        String webasset =  namestub + ".web.jsonlist";
        String graphasset =  namestub + ".graph.jsonlist";
        String hashKey = new MultiProtocolURL(urls.getString(0)).getHost();

        // create actions to be done in reverse order:
        // at the end of the processing we simultaneously place actions on the indexing and crawling queue
        JSONArray postParserActions = new JSONArray();
        assert doIndexing || doCrawling; // one or both must be true; doing none of that does not make sense
        // if all of the urls shall be indexed (see indexing patterns) then do indexing actions
        if (doIndexing) {
            GridQueue indexerQueueName = Data.gridBroker.queueName(YaCyServices.indexer, YaCyServices.indexer.getQueues(), ShardingMethod.BALANCE, INDEXER_PRIORITY_DIMENSIONS, priority, hashKey);
            postParserActions.put(new JSONObject(true)
                .put("type", YaCyServices.indexer.name())
                .put("queue", indexerQueueName.name())
                .put("id", id)
                .put("sourceasset", webasset)
             );
        }
        // if all of the urls shall be crawled at depth + 1, add a crawling action. Don't do this only if the crawling depth is at the depth limit.
        if (doCrawling) {
            GridQueue crawlerQueueName = Data.gridBroker.queueName(YaCyServices.crawler, YaCyServices.crawler.getQueues(), ShardingMethod.BALANCE, CRAWLER_PRIORITY_DIMENSIONS, priority, hashKey);
            postParserActions.put(new JSONObject(true)
                .put("type", YaCyServices.crawler.name())
                .put("queue", crawlerQueueName.name())
                .put("id", id)
                .put("depth", depth + 1)
                .put("sourcegraph", graphasset)
             );
        }
        
        // bevor that and after loading we have a parsing action
        GridQueue parserQueueName = Data.gridBroker.queueName(YaCyServices.parser, YaCyServices.parser.getQueues(), ShardingMethod.BALANCE, PARSER_PRIORITY_DIMENSIONS, priority, hashKey);
        JSONArray parserActions = new JSONArray().put(new JSONObject(true)
                .put("type", YaCyServices.parser.name())
                .put("queue", parserQueueName.name())
                .put("id", id)
                .put("sourceasset", warcasset)
                .put("targetasset", webasset)
                .put("targetgraph", graphasset)
                .put("actions", postParserActions)); // actions after parsing
        
        // at the beginning of the process, we do a loading.
        GridQueue loaderQueueName = Data.gridBroker.queueName(YaCyServices.loader, YaCyServices.loader.getQueues(), ShardingMethod.BALANCE, LOADER_PRIORITY_DIMENSIONS, priority, hashKey);
        JSONObject loaderAction = new JSONObject(true)
            .put("type", YaCyServices.loader.name())
            .put("queue", loaderQueueName.name())
            .put("id", id)
            .put("urls", urls)
            .put("targetasset", warcasset)
            .put("actions", parserActions); // actions after loading
        return loaderAction;
    }
    
    private final static String intf(int i) {
       String s = Integer.toString(i);
       while (s.length() < 3) s = '0' + s;
       return s;
    }
    
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
    
    public static String getCrawlID(MultiProtocolURL url, Date date, int count) {
        String id = url.getHost();
        if (id.length() > 80) id = id.substring(0, 80) + "-" + id.hashCode();
        id = id + "-" + DateParser.secondDateFormat.format(date).replace(':', '-').replace(' ', '-') + "-" + count;
        return id;
    }
    
    public static void main(String[] args) {
        // initialize environment variables
        List<Class<? extends Servlet>> services = new ArrayList<>();
        services.addAll(Arrays.asList(MCP.MCP_SERVICES));
        services.addAll(Arrays.asList(CRAWLER_SERVICES));
        Service.initEnvironment(CRAWLER_SERVICE, services, DATA_PATH);

        // read global blacklists
        String[] grid_crawler_blacklist = Data.config.get("grid.crawler.blacklist").split(",");
        String[] grid_indexer_blacklist = Data.config.get("grid.indexer.blacklist").split(",");
        
        // start listener
        BrokerListener brokerListener = new CrawlerListener(CRAWLER_SERVICE, grid_crawler_blacklist, grid_indexer_blacklist);
        new Thread(brokerListener).start();

        // initialize data
        Data.logger.info("started Crawler");
        Data.logger.info(new GitTool().toString());
        
        int priorityQueues = Integer.parseInt(Data.config.get("grid.indexer.priorityQueues"));
        initPriorityQueue(priorityQueues);
        
        
        // start server
        Service.runService(null);
        brokerListener.terminate();
    }
    
}
