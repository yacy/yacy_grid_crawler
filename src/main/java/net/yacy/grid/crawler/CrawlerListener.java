package net.yacy.grid.crawler;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONObject;

import com.hazelcast.core.HazelcastException;

import ai.susi.mind.SusiAction;
import ai.susi.mind.SusiThought;
import net.yacy.grid.Services;
import net.yacy.grid.YaCyServices;
import net.yacy.grid.contracts.User;
import net.yacy.grid.io.assets.Asset;
import net.yacy.grid.io.index.CrawlerDocument;
import net.yacy.grid.io.index.CrawlerDocument.Status;
import net.yacy.grid.io.index.GridIndex;
import net.yacy.grid.io.index.WebMapping;
import net.yacy.grid.io.messages.GridQueue;
import net.yacy.grid.io.messages.ShardingMethod;
import net.yacy.grid.mcp.AbstractBrokerListener;
import net.yacy.grid.mcp.BrokerListener;
import net.yacy.grid.mcp.Configuration;
import net.yacy.grid.tools.Classification.ContentDomain;
import net.yacy.grid.tools.CronBox.Telemetry;
import net.yacy.grid.tools.DateParser;
import net.yacy.grid.tools.Digest;
import net.yacy.grid.tools.JSONList;
import net.yacy.grid.tools.Logger;
import net.yacy.grid.tools.MultiProtocolURL;


public class CrawlerListener extends AbstractBrokerListener implements BrokerListener {

    private final static String[] FIELDS_IN_GRAPH = new String[]{
            WebMapping.inboundlinks_sxt.name(),
            WebMapping.outboundlinks_sxt.name(),
            //WebMapping.images_sxt.name(),
            WebMapping.frames_sxt.name(),
            WebMapping.iframes_sxt.name()
    };

    private final static String PATTERN_TIMEF = "MMddHHmmssSSS";

    public static int[] CRAWLER_PRIORITY_DIMENSIONS = YaCyServices.crawler.getSourceQueues().length == 1 ? new int[] {1, 0} : new int[] {YaCyServices.crawler.getSourceQueues().length - 1, 1};
    private static int[] LOADER_PRIORITY_DIMENSIONS = YaCyServices.loader.getSourceQueues().length == 1 ? new int[] {1, 0} : new int[] {YaCyServices.loader.getSourceQueues().length - 1, 1};
    private static int[] PARSER_PRIORITY_DIMENSIONS = YaCyServices.parser.getSourceQueues().length == 1 ? new int[] {1, 0} : new int[] {YaCyServices.parser.getSourceQueues().length - 1, 1};
    private static int[] INDEXER_PRIORITY_DIMENSIONS = YaCyServices.indexer.getSourceQueues().length == 1 ? new int[] {1, 0} : new int[] {YaCyServices.indexer.getSourceQueues().length - 1, 1};

    static void initPriorityQueue(final int priorityDimension) {
        CRAWLER_PRIORITY_DIMENSIONS = priorityDimensions(YaCyServices.crawler, priorityDimension);
        LOADER_PRIORITY_DIMENSIONS = priorityDimensions(YaCyServices.loader, priorityDimension);
        PARSER_PRIORITY_DIMENSIONS = priorityDimensions(YaCyServices.parser, priorityDimension);
        INDEXER_PRIORITY_DIMENSIONS = priorityDimensions(YaCyServices.indexer, priorityDimension);
    }

    private static int[] priorityDimensions(final YaCyServices service, final int d) {
        return service.getSourceQueues().length <= d ? new int[] {service.getSourceQueues().length, 0} : new int[] {service.getSourceQueues().length - d, d};
    }

    private final String[] blacklist_crawler_names_list, blacklist_indexer_names_list;
    private final Map<String, Blacklist> blacklists_crawler, blacklists_indexer;

    //private final static Map<String, DoubleCache> doubles = Service.hazelcast.getMap("doubles");
    private final Map<String, DoubleCache> doubles = new ConcurrentHashMap<>();
    private static long doublesLastCleanup = System.currentTimeMillis();
    private final static long doublesCleanupTimeout = 1000L * 60L * 60L * 24L * 7L; // cleanup after 7 days
    private final static long doublesCleanupPeriod = 1000L * 60L * 10L; // do cleanup each 10 minutes
    private static class DoubleCache implements Serializable {
        private static final long serialVersionUID = 614262945713636851L;
        public Set<String> doubleHashes;
        public long time;
        public DoubleCache() {
            this.time = System.currentTimeMillis();
            this.doubleHashes = ConcurrentHashMap.newKeySet();
        }
    }

    private void doDoubleCleanup() {
        final long now = System.currentTimeMillis();
        if (now - doublesLastCleanup < doublesCleanupPeriod) return;
        doublesLastCleanup = now;
        Iterator<Map.Entry<String, DoubleCache>> i = this.doubles.entrySet().iterator();
        while (i.hasNext()) {
            final Map.Entry<String, DoubleCache> cache = i.next();
            if ((now - cache.getValue().time) > doublesCleanupTimeout) {
                cache.getValue().doubleHashes.clear();
                i.remove();
            }
        }
        if (this.config.hazelcast != null) {
            try {
                final Map<String, DoubleCache> hazeldoubles = this.config.hazelcast.getMap("doubles");
                i = hazeldoubles.entrySet().iterator();
                while (i.hasNext()) {
                    final Map.Entry<String, DoubleCache> cache = i.next();
                    if ((now - cache.getValue().time) > doublesCleanupTimeout) {
                        cache.getValue().doubleHashes.clear();
                        i.remove();
                    }
                }
            } catch (final HazelcastException e) {
                Logger.error(e);
                this.config.hazelcast = null; // <VADER>DON'T FAIL ME AGAIN!</VADER>
            }
        }
    }

    public static class CrawlstartURLSplitter {

        private final List<MultiProtocolURL> crawlingURLArray;
        private final List<String> badURLStrings;

        public CrawlstartURLSplitter(String crawlingURLsString) {
            Logger.info(this.getClass(), "splitting url list: " + crawlingURLsString);
            crawlingURLsString = crawlingURLsString.replaceAll("\\|http", "\nhttp").replaceAll("%7Chttp", "\nhttp").replaceAll("%0D%0A", "\n").replaceAll("%0A", "\n").replaceAll("%0D", "\n").replaceAll(" ", "\n");
            final String[] crawlingURLs = crawlingURLsString.split("\n");
            this.crawlingURLArray = new ArrayList<>();
            this.badURLStrings = new ArrayList<>();
            for (final String u: crawlingURLs) {
                try {
                    final MultiProtocolURL url = new MultiProtocolURL(u);
                    Logger.info(this.getClass(), "splitted url: " + url.toNormalform(true));
                    this.crawlingURLArray.add(url);
                } catch (final MalformedURLException e) {
                    this.badURLStrings.add(u);
                    Logger.warn(this.getClass(), "error when starting crawl with splitter url " + u + "; splitted from " + crawlingURLsString, e);
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

    public static String getCrawlID(final MultiProtocolURL url, final Date date, final int count) {
        String id = url.getHost();
        if (id.length() > 80) id = id.substring(0, 80) + "-" + id.hashCode();
        id = id + "-" + DateParser.secondDateFormat.format(date).replace(':', '-').replace(' ', '-') + "-" + count;
        return id;
    }

    public CrawlerListener(final Configuration config, final YaCyServices service) {
        super(config, service, Runtime.getRuntime().availableProcessors());

        this.blacklist_crawler_names_list = config.properties.get("grid.crawler.blacklist").split(",");
        this.blacklist_indexer_names_list = config.properties.get("grid.indexer.blacklist").split(",");
        this.blacklists_crawler = new ConcurrentHashMap<>();
        this.blacklists_indexer = new ConcurrentHashMap<>();
    }

    private final Blacklist getBlacklistCrawler(final String processName, final int processNumber) {
        final String key = processName + "_" + processNumber;
        Blacklist blacklist = this.blacklists_crawler.get(key);
        if (blacklist == null) {
            this.blacklists_crawler.put(key, blacklist = loadBlacklist(this.blacklist_crawler_names_list));
        }
        return blacklist;
    }

    private final Blacklist getBlacklistIndexer(final String processName, final int processNumber) {
        final String key = processName + "_" + processNumber;
        Blacklist blacklist = this.blacklists_indexer.get(key);
        if (blacklist == null) {
            this.blacklists_indexer.put(key, blacklist = loadBlacklist(this.blacklist_indexer_names_list));
        }
        return blacklist;
    }

    private final Blacklist loadBlacklist(final String[] names) {
        final Blacklist blacklist = new Blacklist();
        for (final String name: names) {
            File f = new File(super.config.gridServicePath, "conf/" + name.trim());
            if (!f.exists()) f = new File("conf/" + name.trim());
            if (!f.exists()) continue;
            try {
                blacklist.load(f);
            } catch (final IOException e) {
                Logger.warn(this.getClass(), e);
            }
        }
        return blacklist;
    }

    @Override
    public ActionResult processAction(final SusiAction crawlaction, final JSONArray data, final String processName, final int processNumber) {
        doDoubleCleanup();
        final String crawlID = crawlaction.getStringAttr("id");
        String userId = crawlaction.getStringAttr("userId"); if (userId == null || userId.length() == 0) userId = User.ANONYMOUS_ID;
        if (crawlID == null || crawlID.length() == 0) {
            Logger.info("Crawler.processAction Fail: Action does not have an id: " + crawlaction.toString());
            return ActionResult.FAIL_IRREVERSIBLE;
        }
        final JSONObject crawl = SusiThought.selectData(data, "id", crawlID);
        if (crawl == null) {
            Logger.info(this.getClass(), "Crawler.processAction Fail: ID of Action not found in data: " + crawlaction.toString());
            return ActionResult.FAIL_IRREVERSIBLE;
        }

        final int depth = crawlaction.getIntAttr("depth");
        final int crawlingDepth = crawl.getInt("crawlingDepth");
        final int priority =  crawl.has("priority") ? crawl.getInt("priority") : 0;
        // check depth (this check should be deprecated because we limit by omitting the crawl message at crawl tree leaves)
        if (depth > crawlingDepth) {
            // this is a leaf in the crawl tree (it does not mean that the crawl is finished)
            Logger.info(this.getClass(), "Crawler.processAction Leaf: reached a crawl leaf for crawl " + crawlID + ", depth = " + crawlingDepth);
            return ActionResult.SUCCESS;
        }
        final boolean isCrawlLeaf = depth == crawlingDepth;

        // load graph
        final String sourcegraph = crawlaction.getStringAttr("sourcegraph");
        if (sourcegraph == null || sourcegraph.length() == 0) {
            Logger.info(this.getClass(), "Crawler.processAction Fail: sourcegraph of Action is empty: " + crawlaction.toString());
            return ActionResult.FAIL_IRREVERSIBLE;
        }
        try {
            JSONList jsonlist = null;
            if (crawlaction.hasAsset(sourcegraph)) {
                jsonlist = crawlaction.getJSONListAsset(sourcegraph);
            }
            if (jsonlist == null) try {
                final Asset<byte[]> graphasset = super.config.gridStorage.load(sourcegraph); // this must be a list of json, containing document links
                final byte[] graphassetbytes = graphasset.getPayload();
                jsonlist = new JSONList(new ByteArrayInputStream(graphassetbytes));
            } catch (final IOException e) {
                Logger.warn(this.getClass(), "Crawler.processAction could not read asset from storage: " + sourcegraph, e);
                return ActionResult.FAIL_IRREVERSIBLE;
            }

            // declare filter from the crawl profile
            final String mustmatchs = crawl.optString("mustmatch");
            final Pattern mustmatch = Pattern.compile(mustmatchs);
            final String mustnotmatchs = crawl.optString("mustnotmatch");
            final Pattern mustnotmatch = Pattern.compile(mustnotmatchs);
            // filter for indexing steering
            final String indexmustmatchs = crawl.optString("indexmustmatch");
            final Pattern indexmustmatch = Pattern.compile(indexmustmatchs);
            final String indexmustnotmatchs = crawl.optString("indexmustnotmatch");
            final Pattern indexmustnotmatch = Pattern.compile(indexmustnotmatchs);
            // attributes for new crawl entries
            final String collectionss = crawl.optString("collection");
            final Map<String, Pattern> collections = WebMapping.collectionParser(collectionss);
            final String start_url = crawl.optString("start_url");
            final String start_ssld = crawl.optString("start_ssld");

            final Date now = new Date();
            final long timestamp = now.getTime();
            // For each of the parsed document, there is a target graph.
            // The graph contains all url elements which may appear in a document.
            // In the following loop we collect all urls which may be of interest for the next depth of the crawl.
            final Map<String, String> nextMap = new HashMap<>(); // a map from urlid to url
            final Blacklist blacklist_crawler = getBlacklistCrawler(processName, processNumber);
            final List<CrawlerDocument> crawlerDocuments = new ArrayList<>();
            graphloop: for (int line = 0; line < jsonlist.length(); line++) {
                final JSONObject json = jsonlist.get(line);
                if (json.has("index")) continue graphloop; // this is an elasticsearch index directive, we just skip that

                final String sourceurl = json.has(WebMapping.url_s.getMapping().name()) ? json.getString(WebMapping.url_s.getMapping().name()) : "";
                final Set<MultiProtocolURL> graph = new HashSet<>();
                final String graphurl = json.has(WebMapping.canonical_s.name()) ? json.getString(WebMapping.canonical_s.name()) : null;
                if (graphurl != null) try {
                    graph.add(new MultiProtocolURL(graphurl));
                } catch (final MalformedURLException e) {
                    Logger.warn(this.getClass(), "Crawler.processAction error when starting crawl with canonical url " + graphurl, e);
                }
                for (final String field: FIELDS_IN_GRAPH) {
                    if (json.has(field)) {
                        final JSONArray a = json.getJSONArray(field);
                        urlloop: for (int i = 0; i < a.length(); i++) {
                            final String u = a.getString(i);
                            try {
                                graph.add(new MultiProtocolURL(u));
                            } catch (final MalformedURLException e) {
                                Logger.warn(this.getClass(), "Crawler.processAction we discovered a bad follow-up url: " + u, e);
                                continue urlloop;
                            }
                        }
                    }
                }

                // sort out doubles and apply filters
                DoubleCache doublecache = null;
                if (this.config.hazelcast == null) {
                    if (!this.doubles.containsKey(crawlID)) this.doubles.put(crawlID, new DoubleCache());
                    doublecache = this.doubles.get(crawlID);
                } else {
                    try {
                        final Map<String, DoubleCache> hazeldoubles = this.config.hazelcast.getMap("doubles");
                        if (!hazeldoubles.containsKey(crawlID)) hazeldoubles.put(crawlID, new DoubleCache());
                        doublecache = hazeldoubles.get(crawlID);
                    } catch (final HazelcastException e) {
                        Logger.error(e);
                        this.config.hazelcast = null; // <VADER>DON'T FAIL ME AGAIN!</VADER>
                        if (!this.doubles.containsKey(crawlID)) this.doubles.put(crawlID, new DoubleCache());
                        doublecache = this.doubles.get(crawlID);
                    }
                }
                Logger.info(this.getClass(), "Crawler.processAction processing sub-graph with " + graph.size() + " urls for url " + sourceurl);
                urlcheck: for (final MultiProtocolURL url: graph) {
                    // prepare status document
                    final ContentDomain cd = url.getContentDomainFromExt();

                    if (cd == ContentDomain.TEXT || cd == ContentDomain.ALL) {
                        // check if the url shall be loaded using the constraints
                        final String u = url.toNormalform(true);
                        final String urlid = Digest.encodeMD5Hex(u);

                        // double check with the fast double cache
                        if (doublecache.doubleHashes.contains(urlid)) {
                            continue urlcheck;
                        }
                        doublecache.doubleHashes.add(urlid);

                        // create new crawl status document
                        final CrawlerDocument crawlStatus = new CrawlerDocument()
                                .setCrawlID(crawlID)
                                .setUserlID(userId)
                                .setMustmatch(mustmatchs)
                                .setCollections(collections.keySet())
                                .setCrawlstartURL(start_url)
                                .setCrawlstartSSLD(start_ssld)
                                .setInitDate(now)
                                .setStatusDate(now)
                                .setURL(u);

                        // check matcher rules
                        if (!mustmatch.matcher(u).matches() || mustnotmatch.matcher(u).matches()) {
                            crawlStatus
                                .setStatus(Status.rejected)
                                .setComment(!mustmatch.matcher(u).matches() ? "url does not match must-match filter " + mustmatchs : "url matches mustnotmatch filter " + mustnotmatchs);
                            crawlerDocuments.add(crawlStatus);
                            continue urlcheck;
                        }

                        // check blacklist (this is costly because the blacklist is huge)
                        final Blacklist.BlacklistInfo blacklistInfo = blacklist_crawler.isBlacklisted(u, url);
                        if (blacklistInfo != null) {
                            Logger.info(this.getClass(), "Crawler.processAction crawler blacklist pattern '" + blacklistInfo.matcher.pattern().toString() + "' removed url '" + u + "' from crawl list " + blacklistInfo.source + ":  " + blacklistInfo.info);
                            crawlStatus
                                .setStatus(Status.rejected)
                                .setComment("url matches blacklist");
                            crawlerDocuments.add(crawlStatus);
                            continue urlcheck;
                        }

                        // double check with the elastic index (we do this late here because it is the most costly operation)
                        //if (config.gridIndex.exist(GridIndex.CRAWLER_INDEX_NAME, GridIndex.EVENT_TYPE_NAME, urlid)) {
                        //    continue urlcheck;
                        //}

                        // add url to next stack
                        nextMap.put(urlid, u);
                    }
                };
            }

            if (!nextMap.isEmpty()) {

                // make a double-check
                final String crawlerIndexName = super.config.properties.getOrDefault("grid.elasticsearch.indexName.crawler", GridIndex.DEFAULT_INDEXNAME_CRAWLER);
                final Set<String> exist = super.config.gridIndex.existBulk(crawlerIndexName, nextMap.keySet());
                for (final String u: exist) nextMap.remove(u);
                final Collection<String> nextList = nextMap.values(); // a set of urls

                // divide the nextList into two sub-lists, one which will reach the indexer and another one which will not cause indexing
                @SuppressWarnings("unchecked")
                final
                List<String>[] indexNoIndex = new List[2];
                indexNoIndex[0] = new ArrayList<>(); // for: index
                indexNoIndex[1] = new ArrayList<>(); // for: no-Index
                final Blacklist blacklist_indexer = getBlacklistIndexer(processName, processNumber);
                nextList.forEach(url -> {
                    final boolean indexConstratntFromCrawlProfil = indexmustmatch.matcher(url).matches() && !indexmustnotmatch.matcher(url).matches();
                    final Blacklist.BlacklistInfo blacklistInfo = blacklist_indexer.isBlacklisted(url, null);
                    final boolean indexConstraintFromBlacklist = blacklistInfo == null;
                    if (indexConstratntFromCrawlProfil && indexConstraintFromBlacklist) {
                        indexNoIndex[0].add(url);
                    } else {
                        indexNoIndex[1].add(url);
                    }
                });

                for (int ini = 0; ini < 2; ini++) {

                    // create crawler index entries
                    for (final String u: indexNoIndex[ini]) {
                        final CrawlerDocument crawlStatus = new CrawlerDocument()
                            .setCrawlID(crawlID)
                            .setMustmatch(mustmatchs)
                            .setCollections(collections.keySet())
                            .setCrawlstartURL(start_url)
                            .setCrawlstartSSLD(start_ssld)
                            .setInitDate(now)
                            .setStatusDate(now)
                            .setStatus(Status.accepted)
                            .setURL(u)
                            .setComment(ini == 0 ? "to be indexed" : "noindex, just for crawling");
                        crawlerDocuments.add(crawlStatus);
                    }

                    // create partitions
                    final List<JSONArray> partitions = createPartition(indexNoIndex[ini], 4);

                    // create follow-up crawl to next depth
                    for (int pc = 0; pc < partitions.size(); pc++) {
                        final JSONObject loaderAction = newLoaderAction(priority, crawlID, partitions.get(pc), depth, isCrawlLeaf, 0, timestamp + ini, pc, depth < crawlingDepth, ini == 0); // action includes whole hierarchy of follow-up actions
                        final SusiThought nextjson = new SusiThought()
                                .setData(data)
                                .addAction(new SusiAction(loaderAction));

                        // put a loader message on the queue
                        final String message = nextjson.toString(2);
                        final byte[] b = message.getBytes(StandardCharsets.UTF_8);
                        try {
                            final Services serviceName = YaCyServices.valueOf(loaderAction.getString("type"));
                            final GridQueue queueName = new GridQueue(loaderAction.getString("queue"));
                            super.config.gridBroker.send(serviceName, queueName, b);
                        } catch (final IOException e) {
                            Logger.warn(this.getClass(), "error when starting crawl with message " + message, e);
                        }
                    };
                }
            }
            // bulk-store the crawler documents
            final Map<String, CrawlerDocument> crawlerDocumentsMap = new HashMap<>();
            crawlerDocuments.forEach(crawlerDocument -> {
                final String url = crawlerDocument.getURL();
                    if (url != null && url.length() > 0) {
                        final String id = Digest.encodeMD5Hex(url);
                        crawlerDocumentsMap.put(id, crawlerDocument);
                    } else {
                        assert false : "url not set / storeBulk";
                    }
            });
            CrawlerDocument.storeBulk(super.config, super.config.gridIndex, crawlerDocumentsMap);
            Logger.info(this.getClass(), "Crawler.processAction processed graph with " +  jsonlist.length()/2 + " subgraphs from " + sourcegraph);
            return ActionResult.SUCCESS;
        } catch (final Throwable e) {
            Logger.warn(this.getClass(), "Crawler.processAction Fail: loading of sourcegraph failed: " + e.getMessage() /*+ "\n" + crawlaction.toString()*/, e);
            return ActionResult.FAIL_IRREVERSIBLE;
        }
    }

    private static List<JSONArray> createPartition(final Collection<String> urls, final int partitionSize) {
        final List<JSONArray> partitions = new ArrayList<>();
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

    /**
     * Create a new loader action. This action contains all follow-up actions after
     * loading to create a steering of parser, indexing and follow-up crawler actions.
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
    private JSONObject newLoaderAction(
            final int priority,
            final String id,
            final JSONArray urls,
            final int depth,
            final boolean isCrawlLeaf,
            final int retry,
            final long timestamp,
            final int partition,
            final boolean doCrawling,
            final boolean doIndexing) throws IOException {
        // create file names for the assets: this uses depth and partition information
        final SimpleDateFormat FORMAT_TIMEF = new SimpleDateFormat(PATTERN_TIMEF, Locale.US); // we must create this here to prevent concurrency bugs which are there in the date formatter :((
        final String namestub = id + "/d" + intf(depth) + "-t" + FORMAT_TIMEF.format(new Date(timestamp)) + "-p" + intf(partition);
        final String warcasset =  namestub + ".warc.gz";
        final String webasset =  namestub + ".web.jsonlist";
        final String graphasset =  namestub + ".graph.jsonlist";
        final String hashKey = new MultiProtocolURL(urls.getString(0)).getHost();

        // create actions to be done in reverse order:
        // at the end of the processing we simultaneously place actions on the indexing and crawling queue
        final JSONArray postParserActions = new JSONArray();
        assert doIndexing || doCrawling; // one or both must be true; doing none of that does not make sense
        // if all of the urls shall be indexed (see indexing patterns) then do indexing actions
        if (doIndexing) {
            final GridQueue indexerQueueName = super.config.gridBroker.queueName(YaCyServices.indexer, YaCyServices.indexer.getSourceQueues(), ShardingMethod.LEAST_FILLED, INDEXER_PRIORITY_DIMENSIONS, priority, hashKey);
            postParserActions.put(new JSONObject(true)
                .put("type", YaCyServices.indexer.name())
                .put("queue", indexerQueueName.name())
                .put("id", id)
                .put("sourceasset", webasset)
             );
        }
        // if all of the urls shall be crawled at depth + 1, add a crawling action. Don't do this only if the crawling depth is at the depth limit.
        if (doCrawling) {
            final GridQueue crawlerQueueName = super.config.gridBroker.queueName(YaCyServices.crawler, YaCyServices.crawler.getSourceQueues(), ShardingMethod.LEAST_FILLED, CRAWLER_PRIORITY_DIMENSIONS, priority, hashKey);
            postParserActions.put(new JSONObject(true)
                .put("type", YaCyServices.crawler.name())
                .put("queue", crawlerQueueName.name())
                .put("id", id)
                .put("depth", depth + 1)
                .put("sourcegraph", graphasset)
             );
        }

        // bevor that and after loading we have a parsing action
        final GridQueue parserQueueName = super.config.gridBroker.queueName(YaCyServices.parser, YaCyServices.parser.getSourceQueues(), ShardingMethod.LEAST_FILLED, PARSER_PRIORITY_DIMENSIONS, priority, hashKey);
        final JSONArray parserActions = new JSONArray().put(new JSONObject(true)
                .put("type", YaCyServices.parser.name())
                .put("queue", parserQueueName.name())
                .put("id", id)
                .put("sourceasset", warcasset)
                .put("targetasset", webasset)
                .put("targetgraph", graphasset)
                .put("actions", postParserActions)); // actions after parsing

        // at the beginning of the process, we do a loading.
        final GridQueue loaderQueueName = super.config.gridBroker.queueName(YaCyServices.loader, YaCyServices.loader.getSourceQueues(), isCrawlLeaf ? ShardingMethod.LEAST_FILLED : ShardingMethod.BALANCE, LOADER_PRIORITY_DIMENSIONS, priority, hashKey);
        final JSONObject loaderAction = new JSONObject(true)
            .put("type", YaCyServices.loader.name())
            .put("queue", loaderQueueName.name())
            .put("id", id)
            .put("urls", urls)
            .put("targetasset", warcasset)
            .put("actions", parserActions); // actions after loading
        return loaderAction;
    }

    private final static String intf(final int i) {
       String s = Integer.toString(i);
       while (s.length() < 3) s = '0' + s;
       return s;
    }

    @Override
    public Telemetry getTelemetry() {
        return null;
    }
}
