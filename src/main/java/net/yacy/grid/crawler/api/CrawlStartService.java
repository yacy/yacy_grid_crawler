/**
 *  CrawlStartService
 *  Copyright 12.6.2017 by Michael Peter Christen, @0rb1t3r
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

package net.yacy.grid.crawler.api;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

import ai.susi.mind.SusiAction;
import ai.susi.mind.SusiThought;
import net.yacy.grid.YaCyServices;
import net.yacy.grid.crawler.Crawler;
import net.yacy.grid.crawler.Crawler.CrawlstartURLSplitter;
import net.yacy.grid.http.APIHandler;
import net.yacy.grid.http.ObjectAPIHandler;
import net.yacy.grid.http.Query;
import net.yacy.grid.http.ServiceResponse;
import net.yacy.grid.io.index.CrawlerMapping;
import net.yacy.grid.io.index.CrawlstartDocument;
import net.yacy.grid.io.index.CrawlstartMapping;
import net.yacy.grid.io.index.Index.QueryLanguage;
import net.yacy.grid.io.index.WebMapping;
import net.yacy.grid.io.messages.GridQueue;
import net.yacy.grid.io.messages.ShardingMethod;
import net.yacy.grid.mcp.Data;
import net.yacy.grid.tools.Digest;
import net.yacy.grid.tools.Domains;
import net.yacy.grid.tools.JSONList;
import net.yacy.grid.tools.MultiProtocolURL;

/**
 * 
 * Test URL:
 * http://localhost:8300/yacy/grid/crawler/crawlStart.json?crawlingURL=yacy.net&indexmustnotmatch=.*Mitmachen.*&mustmatch=.*yacy.net.*
 * http://localhost:8300/yacy/grid/crawler/crawlStart.json?crawlingURL=ix.de&crawlingDepth=6&priority=true
 * http://localhost:8300/yacy/grid/crawler/crawlStart.json?crawlingURL=tagesschau.de&loaderHeadless=false
 */
public class CrawlStartService extends ObjectAPIHandler implements APIHandler {

    private static final long serialVersionUID = 8578474303031749879L;
    public static final String NAME = "crawlStart";

    @Override
    public String getAPIPath() {
        return "/yacy/grid/crawler/" + NAME + ".json";
    }

    @Override
    public ServiceResponse serviceImpl(Query call, HttpServletResponse response) {
        JSONObject crawlstart = CrawlerDefaultValuesService.crawlStartDefaultClone();

        // read call attributes using the default crawlstart key names
        for (String key: crawlstart.keySet()) {
            Object object = crawlstart.get(key);
            if (object instanceof String) crawlstart.put(key, call.get(key, crawlstart.getString(key)));
            else if (object instanceof Integer) crawlstart.put(key, call.get(key, crawlstart.getInt(key)));
            else if (object instanceof Long) crawlstart.put(key, call.get(key, crawlstart.getLong(key)));
            else if (object instanceof JSONArray) {
                JSONArray a = crawlstart.getJSONArray(key);
                Object cv = call.get(key);
                if (cv != null) crawlstart.put(key, cv);
            } else {
                System.out.println("unrecognized type: " + object.getClass().toString());
            }
        }

        // fix attributes
        final int crawlingDepth = crawlstart.optInt("crawlingDepth", 3);
        crawlstart.put("crawlingDepth", Math.min(crawlingDepth, 8)); // crawlingDepth shall not exceed 8 - this is used for enhanced balancing to be able to reach crawl leaves
        final String mustmatch = crawlstart.optString("mustmatch", "").trim();
        crawlstart.put("mustmatch", mustmatch);
        final Map<String, Pattern> collections = WebMapping.collectionParser(crawlstart.optString("collection").trim());

        // set the crawl id
        CrawlstartURLSplitter crawlstartURLs = new CrawlstartURLSplitter(crawlstart.getString("crawlingURL"));
        Date now = new Date();
        // start the crawls; each of the url in a separate crawl to enforce parallel loading from different hosts
        SusiThought allCrawlstarts = new SusiThought();
        int count = 0;
        for (MultiProtocolURL url: crawlstartURLs.getURLs()) {
            JSONObject singlecrawl = new JSONObject();
            for (String key: crawlstart.keySet()) singlecrawl.put(key, crawlstart.get(key)); // create a clone of crawlstart
            String crawl_id = Crawler.getCrawlID(url, now, count++);
            String start_url = url.toNormalform(true);
            String start_ssld = Domains.getSmartSLD(url.getHost());
            singlecrawl.put("id", crawl_id);
            singlecrawl.put("start_url", start_url);
            singlecrawl.put("start_ssld", start_ssld);
            
            //singlecrawl.put("crawlingURLs", new JSONArray().put(url.toNormalform(true)));

            try {
                // Create a crawlstart index entry: this will keep track of all crawls that have been started.
                // once such an entry is created, it is never changed or deleted again by any YaCy Grid process.
                CrawlstartDocument crawlstartDoc = new CrawlstartDocument()
                        .setCrawlID(crawl_id)
                        .setMustmatch(mustmatch)
                        .setCollections(collections.keySet())
                        .setCrawlstartURL(start_url)
                        .setCrawlstartSSLD(start_ssld)
                        .setInitDate(now)
                        .setData(singlecrawl);
                crawlstartDoc.store(Data.gridIndex);

                // Create a crawler url tracking index entry: this will keep track of single urls and their status
                // While it is processed. The entry also serves as a double-check entry to terminate a crawl even if the
                // crawler is restarted.

                // delete the start url
                String urlid = Digest.encodeMD5Hex(start_url);
                long deleted = Data.gridIndex.delete(Data.config.get("grid.elasticsearch.indexName.crawler"), QueryLanguage.fields, "{ \"_id\":\"" + urlid + "\"}");
                Data.logger.info("deleted " + deleted + " old crawl index entries for _id");

                // Because 'old' crawls may block new ones we identify possible blocking entries using the mustmatch pattern.
                // We therefore delete all entries with the same mustmatch pattern before a crawl starts.
                if (mustmatch.equals(".*")) {
                    // we cannot delete all wide crawl status urls!
                    JSONList old_crawls = Data.gridIndex.query(Data.config.get("grid.elasticsearch.indexName.crawlstart"), QueryLanguage.fields, "{ \"" + CrawlstartMapping.start_url_s.name() + "\":\"" + start_url + "\"}", 0, 100);
                    // from there we pick out the crawl start id and delete using them
                    for (Object j: old_crawls.toArray()) {
                        String crawlid = ((JSONObject) j).optString(CrawlstartMapping.crawl_id_s.name());
                        if (crawlid.length() > 0) {
                            deleted = Data.gridIndex.delete(Data.config.get("grid.elasticsearch.indexName.crawler"), QueryLanguage.fields, "{ \"" + CrawlerMapping.crawl_id_s.name() + "\":\"" + crawlid + "\"}");
                            Data.logger.info("deleted " + deleted + " old crawl index entries for crawl_id_s");
                        }
                    }
                    // we also delete all entries with same start_url and start_ssld
                    deleted = Data.gridIndex.delete(Data.config.get("grid.elasticsearch.indexName.crawler"), QueryLanguage.fields, "{ \"" + CrawlerMapping.start_url_s.name() + "\":\"" + start_url + "\"}");
                    Data.logger.info("deleted " + deleted + " old crawl index entries for start_url_s");
                    deleted = Data.gridIndex.delete(Data.config.get("grid.elasticsearch.indexName.crawler"), QueryLanguage.fields, "{ \"" + CrawlerMapping.start_ssld_s.name() + "\":\"" + start_ssld + "\"}");
                    Data.logger.info("deleted " + deleted + " old crawl index entries for start_ssld_s");
                } else {
                    // this should fit exactly on the old urls
                    // test url:
                    // curl -s -H 'Content-Type: application/json' -X GET http://localhost:9200/crawler/_search?q=_id:0a800a8ec1cc76b5eb8412ec494babc9 | python3 -m json.tool
                    String deletequery = "{ \"" + CrawlerMapping.mustmatch_s.name() + "\":\"" + mustmatch.replace("\\", "\\\\") + "\"}";
                    deleted = Data.gridIndex.delete(Data.config.get("grid.elasticsearch.indexName.crawler"), QueryLanguage.fields, deletequery);
                    Data.logger.info("deleted " + deleted + " old crawl index entries");
                }
                // we do not create a crawler document entry here because that would conflict with the double check.
                // crawler documents must be written after the double check has happened.

                // create a crawl queue entry
                GridQueue queueName = Data.gridBroker.queueName(YaCyServices.crawler, YaCyServices.crawler.getSourceQueues(), ShardingMethod.BALANCE, Crawler.CRAWLER_PRIORITY_DIMENSIONS, singlecrawl.getInt("priority"), url.getHost());
                SusiThought json = new SusiThought();
                json.setData(new JSONArray().put(singlecrawl));
                JSONObject action = new JSONObject()
                        .put("type", YaCyServices.crawler.name())
                        .put("queue", queueName.name())
                        .put("id", crawl_id)
                        .put("depth", 0)
                        .put("sourcegraph", "rootasset");
                SusiAction crawlAction = new SusiAction(action);
                JSONObject graph = new JSONObject(true).put(WebMapping.canonical_s.getMapping().name(), start_url);
                crawlAction.setJSONListAsset("rootasset", new JSONList().add(graph));
                json.addAction(crawlAction);
                allCrawlstarts.addAction(crawlAction);
                byte[] b = json.toString().getBytes(StandardCharsets.UTF_8);
                Data.gridBroker.send(YaCyServices.crawler, queueName, b);

            } catch (IOException e) {
                Data.logger.warn("error when starting crawl for " + url.toNormalform(true), e);
                allCrawlstarts.put(ObjectAPIHandler.COMMENT_KEY, e.getMessage());
            }
        }

        // construct a crawl start message
        allCrawlstarts.setData(new JSONArray().put(crawlstart));
        allCrawlstarts.put(ObjectAPIHandler.SUCCESS_KEY, allCrawlstarts.getActions().size() > 0);

        // finally add the crawl start on the queue
        return new ServiceResponse(allCrawlstarts);
    }

}

