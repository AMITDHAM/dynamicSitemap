package com.jobtrees.jobpostings.service;

import java.io.IOException;
import java.io.StringWriter;
import java.time.Duration;
import java.util.*;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import java.net.HttpURLConnection;
import java.net.URL;

public class JobtreesElasticSearchService {

    private static final String BUCKET_NAME = "your-bucket";
    private static final String PUBLIC_PATH = "public/";
    private static final String INDEXNOW_API_KEY = "your_indexnow_key";
    private static final String REGION = "us-east-1";
    private static final List<String> INDEXNOW_ENDPOINTS = Arrays.asList(
            "https://api.indexnow.org/indexnow",
            "https://www.bing.com/indexnow"
    );

    private RestHighLevelClient client;
    private S3Client s3Client;

    public JobtreesElasticSearchService() {
        this.client = new RestHighLevelClient(RestClient.builder(HttpHost.create("https://search-jobtrees-iqdimaxupmniwiygtkt7nxj3ku.us-east-1.es.amazonaws.com")));
        this.s3Client = S3Client.builder()
                .region(Region.of(REGION))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("your-access-key", "your-secret-key")))
                .build();
    }

    public void generateSitemapsFromExistingJobs(String indexName, int pageSize) throws Exception {
        int page = 0;
        List<String> validFiles = new ArrayList<>();

        while (true) {
            List<SearchHit> hits = fetchJobs(indexName, page * pageSize, pageSize);
            if (hits.isEmpty()) break;

            String xml = generateSitemapXml(hits);
            String fileName = indexName + "_" + (page + 1) + ".xml";
            uploadToS3(fileName, xml);
            submitToIndexNow("https://www.jobtrees.com/api/sitemap/" + fileName);
            validFiles.add(PUBLIC_PATH + fileName);
            page++;
        }

        generateMainSitemap(validFiles);
    }

    private List<SearchHit> fetchJobs(String index, int from, int size) throws IOException {
        SearchRequest request = new SearchRequest(index);
        SearchSourceBuilder builder = new SearchSourceBuilder();
        builder.query(QueryBuilders.matchAllQuery());
        builder.from(from);
        builder.size(size);
        request.source(builder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        return Arrays.asList(response.getHits().getHits());
    }

    private String generateSitemapXml(List<SearchHit> hits) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.newDocument();

        Element urlset = doc.createElement("urlset");
        urlset.setAttribute("xmlns", "http://www.sitemaps.org/schemas/sitemap/0.9");
        doc.appendChild(urlset);

        for (SearchHit hit : hits) {
            Map<String, Object> source = hit.getSourceAsMap();
            if (!"Active".equalsIgnoreCase((String) source.getOrDefault("status", "Active"))) continue;

            Element url = doc.createElement("url");
            urlset.appendChild(url);

            Element loc = doc.createElement("loc");
            loc.setTextContent("https://www.jobtrees.com/postid/" + hit.getId());
            url.appendChild(loc);

            Element lastmod = doc.createElement("lastmod");
            String updated = Optional.ofNullable((String) source.get("updatedDate"))
                    .orElse((String) source.get("postingDate"));
            lastmod.setTextContent(updated);
            url.appendChild(lastmod);

            url.appendChild(createElement(doc, "changefreq", "daily"));
            url.appendChild(createElement(doc, "priority", "1.0"));
        }

        StringWriter writer = new StringWriter();
        TransformerFactory.newInstance().newTransformer().transform(new DOMSource(doc), new StreamResult(writer));
        return writer.toString();
    }

    private Element createElement(Document doc, String name, String value) {
        Element element = doc.createElement(name);
        element.setTextContent(value);
        return element;
    }

    private void uploadToS3(String fileName, String content) {
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(BUCKET_NAME)
                .key(PUBLIC_PATH + fileName)
                .contentType("application/xml")
                .acl("public-read")
                .build();
        s3Client.putObject(request, RequestBody.fromString(content));
    }

    private void submitToIndexNow(String url) {
        for (String endpoint : INDEXNOW_ENDPOINTS) {
            try {
                URL obj = new URL(endpoint);
                HttpURLConnection conn = (HttpURLConnection) obj.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("Content-Type", "application/json");
                conn.setDoOutput(true);

                String payload = String.format(
                        "{\"host\":\"www.jobtrees.com\",\"key\":\"%s\",\"keyLocation\":\"https://www.jobtrees.com/%s.txt\",\"urlList\":[\"%s\"]}",
                        INDEXNOW_API_KEY, INDEXNOW_API_KEY, url);

                conn.getOutputStream().write(payload.getBytes());
                conn.getResponseCode();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void generateMainSitemap(List<String> filePaths) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        Document doc = builder.newDocument();

        Element urlset = doc.createElement("urlset");
        urlset.setAttribute("xmlns", "http://www.sitemaps.org/schemas/sitemap/0.9");
        doc.appendChild(urlset);

        for (String path : filePaths) {
            Element url = doc.createElement("url");
            urlset.appendChild(url);

            Element loc = doc.createElement("loc");
            loc.setTextContent("https://www.jobtrees.com/api/sitemap/" + path.replace(PUBLIC_PATH, ""));
            url.appendChild(loc);

            url.appendChild(createElement(doc, "lastmod", new Date().toInstant().toString()));
            url.appendChild(createElement(doc, "changefreq", "daily"));
            url.appendChild(createElement(doc, "priority", "1.0"));
        }

        StringWriter writer = new StringWriter();
        TransformerFactory.newInstance().newTransformer().transform(new DOMSource(doc), new StreamResult(writer));
        uploadToS3("sitemap_Alljobs.xml", writer.toString());
    }
}
