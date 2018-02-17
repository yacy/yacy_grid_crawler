/**
 *  Blacklist
 *  Copyright 17.02.2018 by Michael Peter Christen, @0rb1t3r
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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import net.yacy.grid.mcp.Data;
import net.yacy.grid.tools.ARC;
import net.yacy.grid.tools.HashARC;
import net.yacy.grid.tools.MultiProtocolURL;

/**
 * A blacklist class to test if an URL is blacklisted.
 * This class has no object synchronization and it must not be used in concurrent environment.
 * The lack of concurrency is done on purpose. Each concurrent thread must initialize it's own blacklist.
 * This ensures that no concurrency issue appears between threads using the same blacklist.
 */
public class Blacklist {

    private final ARC<String, BlacklistInfo> blacklistHitCache;
    private final ARC<String, Boolean> blacklistMissCache;
    private final List<BlacklistInfo> blacklist;

    public Blacklist() {
        this.blacklist = new ArrayList<>();
        this.blacklistHitCache = new HashARC<>(100000);
        this.blacklistMissCache = new HashARC<>(100000);
    }
    
    public void load(File f) throws IOException {
        final AtomicInteger counter = new AtomicInteger(0);
        Files.lines(f.toPath(), StandardCharsets.UTF_8).forEach(line -> {
            line = line.trim();
            int p = line.indexOf(" #");
            String info = "";
            if (p >= 0) {
                info = line.substring(p + 1).trim();
                line = line.substring(0, p);
            }
            line = line.trim();
            if (!line.isEmpty() && !line.startsWith("#")) {
                if (line.startsWith("host ")) {
                    String host = line.substring(5).trim();
                    try {
                        BlacklistInfo bi = new BlacklistInfo(".*?//" + host + "/.*+", f.getName(), info, host);
                        this.blacklist.add(bi);
                        counter.incrementAndGet();
                    } catch (PatternSyntaxException e) {
                        Data.logger.warn("regex for host in file " + f.getName() + " cannot be compiled: " + line.substring(5).trim());
                    }
                } else {
                    try {
                        BlacklistInfo bi = new BlacklistInfo(line, f.getName(), info, null);
                        this.blacklist.add(bi);
                        counter.incrementAndGet();
                    } catch (PatternSyntaxException e) {
                        Data.logger.warn("regex for url in file " + f.getName() + " cannot be compiled: " + line);
                    }
                }
            }
        });
        Data.logger.info("loaded " + counter.get() + " blacklist entries from file " + f.getName());
    }
    
    public final static class BlacklistInfo {
        public final Matcher matcher;
        public final String source;
        public final String info;
        public final String host;
        public BlacklistInfo(final String patternString, final String source, final String info, final String host) throws PatternSyntaxException {
            this.matcher = Pattern.compile(patternString).matcher("");
            this.source = source;
            this.info = info;
            this.host = host;
        }
    }
    
    public BlacklistInfo isBlacklisted(String url, MultiProtocolURL u) {
        BlacklistInfo cachedBI = blacklistHitCache.get(url);
        if (cachedBI != null) return cachedBI;
        Boolean cachedMiss = blacklistMissCache.get(url);
        if (cachedMiss != null) return null;
        for (BlacklistInfo bi: this.blacklist) {
            if (u != null && bi.host != null) {
                if (u.getHost().equals(bi.host)) {
                    return bi;
                }
            } else {
                bi.matcher.reset(url);
                //Thread.currentThread().setName(bi.matcher.pattern().pattern() + " -> " + url);
                if (bi.matcher.matches()) {
                    blacklistHitCache.put(url, bi);
                    return bi;
                }
            }
        }
        blacklistMissCache.put(url, Boolean.TRUE);
        return null;
    }
    
}
