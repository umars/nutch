/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.indexer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.*;
import org.apache.nutch.crawl.*;
import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseImpl;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.scoring.ScoringFilterException;
import org.apache.nutch.scoring.ScoringFilters;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.Collection;
import java.util.Iterator;

public class IndexerMapReduce extends Configured
        implements Mapper<Text, Writable, Text, NutchWritable>,
        Reducer<Text, NutchWritable, Text, NutchDocument> {

    public static final Log LOG = LogFactory.getLog(IndexerMapReduce.class);
    public static final String rregex = "[\\p{InCombiningDiacriticalMarks}\\p{IsLm}\\p{IsSk}]+";
    public static CharsetDecoder utf8Decoder = Charset.forName("UTF-8").newDecoder();


    private IndexingFilters filters;
    private ScoringFilters scfilters;

    public void configure(JobConf job) {
        setConf(job);
        this.filters = new IndexingFilters(getConf());
        this.scfilters = new ScoringFilters(getConf());
        utf8Decoder.onMalformedInput(CodingErrorAction.IGNORE);
        utf8Decoder.onUnmappableCharacter(CodingErrorAction.IGNORE);
    }

    public void map(Text key, Writable value,
                    OutputCollector<Text, NutchWritable> output, Reporter reporter) throws IOException {
        output.collect(key, new NutchWritable(value));
    }

    public void reduce(Text key, Iterator<NutchWritable> values,
                       OutputCollector<Text, NutchDocument> output, Reporter reporter)
            throws IOException {
        Inlinks inlinks = null;
        CrawlDatum dbDatum = null;
        CrawlDatum fetchDatum = null;
        ParseData parseData = null;
        ParseText parseText = null;
        Content rawContent = null;
        while (values.hasNext()) {
            final Writable value = values.next().get(); // unwrap
            if (value instanceof Inlinks) {
                inlinks = (Inlinks)value;
            } else if (value instanceof Content) {
                rawContent = (Content)value;
            } else if (value instanceof CrawlDatum) {
                final CrawlDatum datum = (CrawlDatum)value;
                if (CrawlDatum.hasDbStatus(datum))
                    dbDatum = datum;
                else if (CrawlDatum.hasFetchStatus(datum)) {
                    // don't index unmodified (empty) pages
                    if (datum.getStatus() != CrawlDatum.STATUS_FETCH_NOTMODIFIED)
                        fetchDatum = datum;
                } else if (CrawlDatum.STATUS_LINKED == datum.getStatus() ||
                        CrawlDatum.STATUS_SIGNATURE == datum.getStatus() ||
                        CrawlDatum.STATUS_PARSE_META == datum.getStatus()) {
                    continue;
                } else {
                    throw new RuntimeException("Unexpected status: "+datum.getStatus());
                }
            } else if (value instanceof ParseData) {
                parseData = (ParseData)value;
            } else if (value instanceof ParseText) {
                parseText = (ParseText)value;
            } else if (LOG.isWarnEnabled()) {
                LOG.warn("Unrecognized type: "+value.getClass());
            }
        }

        if (fetchDatum == null || dbDatum == null
                || parseText == null || parseData == null) {
            return;                                     // only have inlinks
        }

        if (!parseData.getStatus().isSuccess() ||
                fetchDatum.getStatus() != CrawlDatum.STATUS_FETCH_SUCCESS) {
            return;
        }

        NutchDocument doc = new NutchDocument();
        final Metadata metadata = parseData.getContentMeta();
        Metadata mdt = parseData.getParseMeta();
        String oce = "OriginalCharEncoding";
        String cef = "CharEncodingForConversion";
        String ce = "Content-Encoding";
        System.out.println("-----------------\n" );
        //System.out.println(oce + "  |  " + mdt.get(oce));
        //System.out.println(cef + "  |  " + mdt.get(cef));
        //System.out.println( ce + "  |  " + mdt.get(ce));
//    String encoding = mdt.get(cef);
//
//    if (encoding == null){
//        System.out.println(oce + "  |  " + mdt.get(oce));
//        System.out.println( ce + "  |  " + mdt.get(ce));
//        encoding = mdt.get(oce);
//        if (encoding == null){
//        encoding = "UTF-8";
//        }
//    }
        /* string normalization to get rid of unicode decode errors
        String htmlDoc = "";
        String s1 = Normalizer.normalize(htmlDoc, Normalizer.Form.NFKD);

        htmlDoc = new String(s1.replaceAll(regex, "").getBytes("ascii"), "ascii");
        */
        // add segment, used to map from merged index back to segment files
        doc.add("segment", metadata.get(Nutch.SEGMENT_NAME_KEY));

        // add digest, used by dedup
        doc.add("digest", metadata.get(Nutch.SIGNATURE_KEY));

        // add raw html content to doc
        if (rawContent != null){
            //String raw_html = Normalizer.normalize(new String(rawContent.getContent(), encoding), Normalizer.Form.NFKD);

            ByteBuffer raw_data = ByteBuffer.wrap(rawContent.getContent());
            //CharBuffer cr_html = utf8Decoder.decode(raw_data);
            doc.add("raw_content" , raw_data);
            doc.add("content_type", rawContent.getContentType());
            LOG.warn("RAW Content ADDED" );
        } else {
            LOG.warn("No RAW Content " );
        }

        final Parse parse = new ParseImpl(parseText, parseData);
        try {
            // extract information from dbDatum and pass it to
            // fetchDatum so that indexing filters can use it
            final Text url = (Text) dbDatum.getMetaData().get(Nutch.WRITABLE_REPR_URL_KEY);
            if (url != null) {
                fetchDatum.getMetaData().put(Nutch.WRITABLE_REPR_URL_KEY, url);
            }
            // run indexing filters
            doc = this.filters.filter(doc, parse, key, fetchDatum, inlinks);
        } catch (final IndexingException e) {
            if (LOG.isWarnEnabled()) { LOG.warn("Error indexing "+key+": "+e); }
            return;
        }

        // skip documents discarded by indexing filters
        if (doc == null) return;

        float boost = 1.0f;
        // run scoring filters
        try {
            boost = this.scfilters.indexerScore(key, doc, dbDatum,
                    fetchDatum, parse, inlinks, boost);
        } catch (final ScoringFilterException e) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Error calculating score " + key + ": " + e);
            }
            return;
        }
        // apply boost to all indexed fields.
        doc.setWeight(boost);
        // store boost for use by explain and dedup
        doc.add("boost", Float.toString(boost));

        output.collect(key, doc);
    }

    public void close() throws IOException { }

    public static void initMRJob(Path crawlDb, Path linkDb,
                                 Collection<Path> segments,
                                 JobConf job) {

        LOG.info("IndexerMapReduce: crawldb: " + crawlDb);
        LOG.info("IndexerMapReduce: linkdb: " + linkDb);
        LOG.info("IndexerMapReduce: geek4377 MOD mult red: ");

        for (final Path segment : segments) {
            LOG.info("IndexerMapReduces: adding segment: " + segment);
            FileInputFormat.addInputPath(job, new Path(segment, CrawlDatum.FETCH_DIR_NAME));
            FileInputFormat.addInputPath(job, new Path(segment, CrawlDatum.PARSE_DIR_NAME));
            FileInputFormat.addInputPath(job, new Path(segment, ParseData.DIR_NAME));
            FileInputFormat.addInputPath(job, new Path(segment, ParseText.DIR_NAME));
            FileInputFormat.addInputPath(job, new Path(segment, Content.DIR_NAME));
        }

        FileInputFormat.addInputPath(job, new Path(crawlDb, CrawlDb.CURRENT_NAME));
        FileInputFormat.addInputPath(job, new Path(linkDb, LinkDb.CURRENT_NAME));
        job.setInputFormat(SequenceFileInputFormat.class);

        job.setMapperClass(IndexerMapReduce.class);
        job.setReducerClass(IndexerMapReduce.class);
        job.setNumReduceTasks(8);

        job.setOutputFormat(IndexerOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NutchWritable.class);
        job.setOutputValueClass(NutchWritable.class);
    }
}
