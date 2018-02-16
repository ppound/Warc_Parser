package ca.upei;

import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;

import java.io.*;
import java.net.URL;

public class ParseWarc {

    public static void main(String[] args) {
        if (args.length < 2) {
            ParseWarc.printUsage();
            return;
        }
        try {
            new ParseWarc().processWarcFile(args[0], args[1]);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void printUsage() {
        System.out.println("usage: java -jar Warc_Parser-1.0-SNAPSHOT-jar-with-dependencies.jar url_to_warc_content object_pid");
        System.out.println("-------------");
        System.out.println("example: java -jar Warc_Parser-1.0-SNAPSHOT-jar-with-dependencies.jar http://localhost:8080/fedora/objects/islandora:1/datastreams/WARC_FILTERED/content islandora:1");
        System.out.println("-------------");
        System.out.println("description: this script assumes the solr server exists at http://localhost:8080/solr/collection1.  If it exists elsewhere you will need" +
                " to set an env variable named Islandora_Solr_Server with a value of the URL that points to your solr server. ");
    }

    /**
     * Process a WARC file by parsing it and index the text of each html body record.
     *
     * @param url The URL of the WARC file
     * @param pid The PID of the Islandora object that contains the WARC file.
     */
    public void processWarcFile(String url, String pid) throws Exception {
            String solrServerUrl = (System.getenv("Islandora_Solr_Server") == null) ? "http://localhost:8080/solr/collection1"
                    : System.getenv("Islandora_Solr_Server");
            System.out.println("using Solr url of " + solrServerUrl);

            SolrServer server = new HttpSolrServer(solrServerUrl);
            // for newer solr with more recent SolrJ we would create a client like below.
            //HttpSolrClient.Builder builder = new HttpSolrClient.Builder(solrServerUrl);
            //depending on solr version we may need this line as well
            //builder.withResponseParser(new XMLResponseParser());
            //SolrClient client = builder.build();

            InputStream in = new URL(url).openStream();

            int records = 0;
            int errors = 0;

            WarcReader reader = WarcReaderFactory.getReader(in);
            WarcRecord record;
            String line;

            while ((record = reader.getNextRecord()) != null) {

                StringBuffer content = new StringBuffer();
                BufferedReader contentReader = new BufferedReader(new InputStreamReader(record.getPayloadContent()));
                while ((line = contentReader.readLine()) != null) {
                    content.append(line);
                }
                String targetUri = record.header.warcTargetUriStr;
                this.addSolrDoc(targetUri, content.toString(), pid, server, records);
                ++records;
            }

            System.out.println("--------------");
            System.out.println("       Records: " + records);
            System.out.println("        Errors: " + errors);
            reader.close();
            in.close();
    }

    /**
     * Indexs a WARC record in Solr
     *
     * @param targetUri The targetUri of the WARC record
     * @param content   The html content of the WARC record
     * @param pid       The pid of the Islandora Object that contains the WARC file.
     * @param server    A SolrJ server object
     * @param records   The record we are processing
     * @throws SolrServerException
     * @throws IOException
     */
    protected void addSolrDoc(String targetUri, String content, String pid, SolrServer server, int records
    ) throws SolrServerException, IOException {
        Document htmlDoc = Jsoup.parse(content);
        Elements body = htmlDoc.select("body");
        Elements title = htmlDoc.select("title");
        String titleContent = title.text();
        String bodyContent = body.text();
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("warc_html_content_t", content);
        doc.addField("warc_html_title", titleContent);
        doc.addField("warc_target_uri_s", targetUri);
        doc.addField("PID", pid + targetUri);
        doc.addField("WARC_PID_s", pid);
        doc.addField("warc_body_content_t", bodyContent);
        doc.addField("doc_type_s", "WARC_DOC");
        doc.addField("warc_record_id_s", Integer.toString(records) + "-" + pid);
        server.add(doc);

        System.out.println("Added Solr Doc " + pid + " " + targetUri);
        if (records % 100 == 0) {
            server.commit();
            System.out.println("Committed Solr changes");
        }

    }


}
