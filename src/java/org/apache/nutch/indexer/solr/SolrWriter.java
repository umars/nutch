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
package org.apache.nutch.indexer.solr;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.NutchField;
import org.apache.nutch.indexer.NutchIndexWriter;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

public class SolrWriter implements NutchIndexWriter {

    private SolrServer solr;
    private SolrMappingReader solrMapping;
    CharsetDecoder utf8Decoder;

    private final List<SolrInputDocument> inputDocs =
            new ArrayList<SolrInputDocument>();

    private int commitSize;
    public static Log LOG = LogFactory.getLog( SolrWriter.class);

    public void open(JobConf job, String name) throws IOException {
        solr = new CommonsHttpSolrServer(job.get(SolrConstants.SERVER_URL));
        commitSize = job.getInt(SolrConstants.COMMIT_SIZE, 100);
        solrMapping = SolrMappingReader.getInstance(job);
        utf8Decoder = Charset.forName("UTF-8").newDecoder();
        utf8Decoder.onMalformedInput(CodingErrorAction.IGNORE);
        utf8Decoder.onUnmappableCharacter(CodingErrorAction.IGNORE);

    }

    public void write(NutchDocument doc) throws IOException {
        final SolrInputDocument inputDoc = new SolrInputDocument();
        for(final Entry<String, NutchField> e : doc) {
            for (final Object val : e.getValue().getValues()) {
                if (e.getKey().equalsIgnoreCase("raw_content") || e.getKey().equalsIgnoreCase("content")){
                    ByteBuffer bbuf;
                    if (val instanceof String){
                        String cont = (String) val;
                        bbuf = ByteBuffer.wrap(cont.getBytes("UTF-8"));
                    } else {
                        bbuf = (ByteBuffer) val;
                    }
                    CharBuffer cr_html = utf8Decoder.decode(bbuf);
                    inputDoc.addField(solrMapping.mapKey(e.getKey()), cr_html.toString(), e.getValue().getWeight());
                } else {
                    inputDoc.addField(solrMapping.mapKey(e.getKey()), val, e.getValue().getWeight());
                }

                String sCopy = solrMapping.mapCopyKey(e.getKey());
                if (sCopy != e.getKey()) {
                    inputDoc.addField(sCopy, val, e.getValue().getWeight());
                }
            }
        }
        inputDoc.setDocumentBoost(doc.getWeight());
        inputDocs.add(inputDoc);
        if (inputDocs.size() > commitSize) {
            try {
                solr.add(inputDocs);
            } catch (SolrException sex){
                LOG.error("SolrException", sex);
            } catch (final SolrServerException e) {
                LOG.error("SolrServerException", makeIOException(e));
            }
            inputDocs.clear();
        }
    }

    public void close() throws IOException {
        try {
            if (!inputDocs.isEmpty()) {
                solr.add(inputDocs);
                inputDocs.clear();
            }
            solr.commit();
        }  catch (SolrException sex){
            LOG.error("SolrException", sex);
        } catch (final SolrServerException e) {
            LOG.error("SolrServerException", makeIOException(e));
        }
    }

    public static IOException makeIOException(SolrServerException e) {
        final IOException ioe = new IOException();
        ioe.initCause(e);
        return ioe;
    }

}
