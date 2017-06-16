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

import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;

import net.yacy.grid.http.APIHandler;
import net.yacy.grid.http.ObjectAPIHandler;
import net.yacy.grid.http.Query;
import net.yacy.grid.http.ServiceResponse;

/**
 * 
 * Test URL:
 * http://localhost:8300/yacy/grid/crawler/crawlStart.json?url=yacy.net
 * 
 * Test command:
 * curl http://localhost:8300/yacy/grid/crawler/crawlStart.json
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
        JSONObject json = CrawlerDefaultValuesService.crawlStartDefault();
        //for (String param: json.keySet()) json.put(param, call.get(param, json.get(param)));
        return new ServiceResponse(json);
    }

}

