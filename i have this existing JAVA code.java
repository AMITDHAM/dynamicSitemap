i have this existing JAVA code

package com.jobtrees.jobpostings.service;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.util.json.Jackson;
import com.jobtrees.jobpostings.common.JobtreesConstants;
import com.jobtrees.jobpostings.model.ESJob;
import com.jobtrees.jobpostings.model.Job;
import com.jobtrees.jobpostings.model.SuggestionModel;

public class JobtreesElasticSearchService {

	private static final String PRD_USER_PROFILE_ELASTIC_URL = "https://search-jobtrees-iqdimaxupmniwiygtkt7nxj3ku.us-east-1.es.amazonaws.com";
	private static final String STG_USER_PROFILE_ELASTIC_URL = "https://search-stageelatic-fegphos2kqdtkacicwzq3izmpq.us-east-1.es.amazonaws.com";

	private static LambdaLogger LOGGER;
	private static JobtreesElasticSearchService INSTANCE;

	private final String elasticSearchUrl;

	private final RestHighLevelClient client;

	private JobtreesElasticSearchService(LambdaLogger logger, String serverEnvironment) {
		LOGGER = logger;
		if ("production".equalsIgnoreCase(serverEnvironment)) {
			elasticSearchUrl = PRD_USER_PROFILE_ELASTIC_URL;
			client = elasticsearchClient(JobtreesConstants.PRD_AWS_ACCESS_KEY, JobtreesConstants.PRD_AWS_SECRET_KEY);
		} else {
			elasticSearchUrl = STG_USER_PROFILE_ELASTIC_URL;
			client = elasticsearchClient(JobtreesConstants.STG_AWS_ACCESS_KEY, JobtreesConstants.STG_AWS_SECRET_KEY);
		}
	}

	public static JobtreesElasticSearchService getInstance(LambdaLogger logger, String serverEnvironment) {
		if (INSTANCE == null) {
			INSTANCE = new JobtreesElasticSearchService(logger, serverEnvironment);
		}
		return INSTANCE;
	}

	public void shutdown() {
		try {
			client.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		INSTANCE = null;
	}

	public void indexRecords(List<Map<String, Object>> listOfRecords, String index) {
		BulkRequest request = new BulkRequest();
		listOfRecords.forEach(record -> {
			UpdateRequest indexRequest = new UpdateRequest();
			indexRequest.index(index).id(record.get("id").toString())
					.doc(Jackson.toJsonString(record), XContentType.JSON).docAsUpsert(true);
			request.add(indexRequest);
			// LOGGER.log("Index request: " + indexRequest);
		});

		executeRequest(request, 0);
	}

	public void indexDeleteRecords(List<String> listOfIds, String index) {
		BulkRequest request = new BulkRequest();
		listOfIds.forEach(id -> {
			DeleteRequest deleteRequest = new DeleteRequest();
			deleteRequest.index(index).id(id);
			request.add(deleteRequest);
			// LOGGER.log("Index request: " + indexRequest);
		});

		executeRequest(request, 0);
	}

	public void deleteIndex(String index) {
		DeleteIndexRequest request = new DeleteIndexRequest(index);
		try {
			AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
			System.err.println("DeleteIndex Response : " + response);
		} catch (Exception e) {
			LOGGER.log("Exception occurred: " + e);
			e.printStackTrace();
		}
	}

	public void deleteExpiredIndex(String index, long timeinMilliSecond) {
		try {
			DeleteByQueryRequest request = new DeleteByQueryRequest(index);
			BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
			boolQueryBuilder.filter(QueryBuilders.rangeQuery("postingDate").lte(timeinMilliSecond));
			request.setQuery(boolQueryBuilder);

			System.err.println(request.getSearchRequest());
			BulkByScrollResponse response = client.deleteByQuery(request, RequestOptions.DEFAULT);
			System.err.println("DeleteIndex Response : " + response);
		} catch (Exception e) {
			LOGGER.log("Exception occurred: " + e);
			e.printStackTrace();
		}
	}

	public void indexSuggestionRecords(Map<String, Map<String, Object>> suggestionModels) {
		BulkRequest request = new BulkRequest();
		suggestionModels.forEach((key, value) -> {
			UpdateRequest indexRequest = new UpdateRequest();
			indexRequest.index("company_suggestion_index").id(key).doc(Jackson.toJsonString(value), XContentType.JSON)
					.docAsUpsert(true);
			request.add(indexRequest);
			LOGGER.log("Index request: " + indexRequest);
		});

		executeRequest(request, 0);
	}

	private void executeRequest(BulkRequest request, int retryAttempt) {
		List<String> failedIds = new ArrayList<>();
		BulkResponse response = null;
		try {
			response = client.bulk(request, RequestOptions.DEFAULT);
		} catch (IOException e) {
			LOGGER.log("IOException occurred: " + e);
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.log("Exception occured: " + e);
			if (retryAttempt <= 3) {
				LOGGER.log("Retrying...");
				executeRequest(request, retryAttempt + 1);
			} else {
				LOGGER.log("Retrying falied. Retry attempt is greater than 3");
			}
		}
		if (response != null) {
			BulkItemResponse[] items = response.getItems();
			for (BulkItemResponse item : items) {
				if (item.getFailure() != null) {
					LOGGER.log("Could not index item : " + item.getFailure().getId());
					LOGGER.log("Reason : " + item.getFailure() + "\n");
					failedIds.add(item.getFailure().getId());
				}
			}
		}
		LOGGER.log("List of failed Ids : " + failedIds);
	}

	private ArrayList<SearchHit> executeSearch(SearchRequest searchRequest, int retryAttempt)
			throws IOException, InterruptedException {

		final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(10L));
		searchRequest.scroll(scroll);

		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		String scrollId = searchResponse.getScrollId();

		ArrayList<SearchHit> allHits = new ArrayList<SearchHit>();

		SearchHit[] searchHits = searchResponse.getHits().getHits();

		while (searchHits != null && searchHits.length > 0) {

			allHits.addAll(Arrays.asList(searchResponse.getHits().getHits()));

			SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);

			scrollRequest.scroll(scroll);

			searchResponse = client.searchScroll(scrollRequest, RequestOptions.DEFAULT);

			scrollId = searchResponse.getScrollId();

			searchHits = searchResponse.getHits().getHits();

			Thread.sleep(100);

		}

		ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
		clearScrollRequest.addScrollId(scrollId);
		ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);

		return allHits;
	}

	public ArrayList<SearchHit> getExpiredRecords(String index, long timeInMillis)
			throws IOException, InterruptedException {
		SearchSourceBuilder builder = new SearchSourceBuilder();

		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		boolQueryBuilder.filter(QueryBuilders.rangeQuery("postingDate").lte(timeInMillis));

		builder.fetchSource(new String[] { "id" }, null);
		builder.query(boolQueryBuilder);
		builder.size(5000); // max is 10000

		SearchRequest searchRequest = new SearchRequest();
		searchRequest.indices(index).source(builder);

		System.err.println(searchRequest);

		ArrayList<SearchHit> searchHits = executeSearch(searchRequest, 3);
		System.out.println("Expired records in ES :  " + searchHits.size());

		return searchHits;
	}

	public ArrayList<SearchHit> getRecords(String index) throws IOException, InterruptedException {
		SearchSourceBuilder builder = new SearchSourceBuilder();
		MatchAllQueryBuilder matchAllQuery = QueryBuilders.matchAllQuery();
		builder.query(matchAllQuery);
		builder.size(5000); // max is 10000

		SearchRequest searchRequest = new SearchRequest();
		searchRequest.indices(index).source(builder);

		ArrayList<SearchHit> searchHits = executeSearch(searchRequest, 3);

		return searchHits;
	}

	public ArrayList<SearchHit> getAllRecords(String index) throws IOException, InterruptedException {
		SearchSourceBuilder builder = new SearchSourceBuilder();
		MatchAllQueryBuilder matchAllQuery = QueryBuilders.matchAllQuery();

		builder.fetchSource(new String[] { "id", "title", "company", "postingDate", "city", "state", "salarymin",
				"salarymax", "salarytext", "postcode" }, null);
		builder.query(matchAllQuery);
		builder.size(5000); // max is 10000

		SearchRequest searchRequest = new SearchRequest();
		searchRequest.indices(index).source(builder);

		ArrayList<SearchHit> searchHits = executeSearch(searchRequest, 3);

		return searchHits;
	}

	public ArrayList<SearchHit> getJobPostingsByTtitles(String index, String title, List<String> titles)
			throws IOException, InterruptedException {

		SearchRequest searchRequest = new SearchRequest(index);
		SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

		boolQuery.should(QueryBuilders.matchPhraseQuery("title", title));

		for (String ttl : titles) {
			boolQuery.should(QueryBuilders.matchPhraseQuery("title", ttl));
		}
		boolQuery.minimumShouldMatch(1);
		sourceBuilder.fetchSource(new String[] { "id", "title" }, null);
		sourceBuilder.query(boolQuery);
		sourceBuilder.size(500); // max is 10000
		searchRequest.indices(index).source(sourceBuilder);

		return executeSearch(searchRequest, 3);
	}

	private RestHighLevelClient elasticsearchClient(String accessKey, String secretKey) {
		AWS4Signer signer = new AWS4Signer();
		String serviceName = "es";
		signer.setServiceName(serviceName);
		signer.setRegionName("us-east-1");
		HttpRequestInterceptor interceptor = new ElasticSearchRequestInterceptor(serviceName, signer,
				new BasicAWSCredentials(accessKey, secretKey));

		return new RestHighLevelClient(RestClient.builder(HttpHost.create(elasticSearchUrl))
				.setHttpClientConfigCallback(callback -> callback.addInterceptorLast(interceptor))
				.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
						.setConnectTimeout((int) Duration.ofHours(2).toMillis())
						.setSocketTimeout((int) Duration.ofHours(2).toMillis())));
	}

	public Map<String, Map<String, Object>> getMappedCompanySuggestionModel(Set<String> companies) {

		Map<String, Map<String, Object>> map = new HashMap<String, Map<String, Object>>();

		for (String company : companies) {
			SuggestionModel companySuggestion = new SuggestionModel();
			companySuggestion.getInput().add(company);
			Map<String, Object> mappedSuggestionRecord = new HashMap<>();
			mappedSuggestionRecord.put("suggestCompany", companySuggestion);
			map.put(company, mappedSuggestionRecord);
		}

		return map;
	}

	public Map<String, List<String>> getRolesFromElasticSearchForGivenTitle(String title)
			throws IOException, InterruptedException {
		Map<String, List<String>> roleMap = new HashMap<String, List<String>>();

		SearchSourceBuilder builder = new SearchSourceBuilder();
		MatchAllQueryBuilder matchAllQuery = QueryBuilders.matchAllQuery();

		builder.fetchSource(new String[] { "title" }, null);
		builder.query(matchAllQuery);
		builder.size(5000); // max is 10000

		SearchRequest searchRequest = new SearchRequest();
		searchRequest.indices("roles_index_03_05_2023").source(builder);

		ArrayList<SearchHit> searchHits = executeSearch(searchRequest, 3);
		for (SearchHit searchHit : searchHits) {
			String roleTitle = (String) searchHit.getSourceAsMap().get("title");
			if (roleTitle != null) {
				List<String> jobtreesTitles = new ArrayList<String>();
				roleMap.put(roleTitle.toLowerCase(), jobtreesTitles);
			}
		}

		DynamoDBService dbService = DynamoDBService.getInstance();

		dbService.getAllRecords("subtitleMapping").forEach(row -> {
			List<String> subTtitles = roleMap.get(row.get("mainTitle").getS().trim().toLowerCase());
			if (subTtitles != null) {
				subTtitles.add(row.get("subTitle").getS().trim().toLowerCase());
			}
		});

		dbService.getAllRecords("mappingtable").forEach(row -> {

			List<String> subTtitles = roleMap.get(row.get("newtitle").getS().trim().toLowerCase());
			if (subTtitles != null) {
				subTtitles.add(row.get("oldtitle").getS().trim().toLowerCase());
			}
		});

		System.out.println("Total role mappings : " + roleMap.size());

		// shutdown();
		return roleMap;
	}

	public ArrayList<String> getEducationFromElasticSearch() throws IOException, InterruptedException {
		SearchSourceBuilder builder = new SearchSourceBuilder();
		MatchAllQueryBuilder matchAllQuery = QueryBuilders.matchAllQuery();

		builder.fetchSource(new String[] { "commonDegree" }, null);
		builder.query(matchAllQuery);
		builder.size(5000); // max is 10000

		SearchRequest searchRequest = new SearchRequest();
		// searchRequest.indices("education_data").source(builder);
		searchRequest.indices("roles_index_03_05_2023").source(builder);

		ArrayList<SearchHit> searchHits = executeSearch(searchRequest, 3);
		ArrayList<String> roles = new ArrayList<>();
		for (SearchHit searchHit : searchHits) {
			String roleTitle = (String) searchHit.getSourceAsMap().get("commonDegree");
			if (roleTitle != null) {
				roles.add(roleTitle);
			}
		}

		shutdown();
		return roles;
	}

	public ArrayList<String> getIndustriesFromElasticSearch() throws IOException, InterruptedException {
		SearchSourceBuilder builder = new SearchSourceBuilder();
		MatchAllQueryBuilder matchAllQuery = QueryBuilders.matchAllQuery();

		builder.fetchSource(new String[] { "industries" }, null);
		builder.query(matchAllQuery);
		builder.size(5000); // max is 10000

		SearchRequest searchRequest = new SearchRequest();
		// searchRequest.indices("education_data").source(builder);
		searchRequest.indices("roles_index_03_05_2023").source(builder);

		ArrayList<SearchHit> searchHits = executeSearch(searchRequest, 3);
		ArrayList<String> roles = new ArrayList<>();
		for (SearchHit searchHit : searchHits) {
			List<String> industries = (List<String>) searchHit.getSourceAsMap().get("industries");
			if (industries != null) {
				roles.addAll(industries);
			}
		}

		shutdown();
		return roles;
	}

	public Set<Job> getAllJobPostings(String index) throws IOException, InterruptedException {
		Set<Job> esJobList = new HashSet<Job>();
		ArrayList<SearchHit> hits = getAllRecords(index);

		for (SearchHit hit : hits) {
			Map<String, Object> source = hit.getSourceAsMap();
			Job job = new Job(hit.getId());
			if (source.get("postingDate") != null) {
				job.setImportdate(source.get("postingDate").toString());
			}
			if (source.get("title") != null) {
				job.setTitle(source.get("title").toString());
			}
			if (source.get("company") != null) {
				job.setCompany(source.get("company").toString());
			}
			if (source.get("city") != null) {
				job.setCity(source.get("city").toString());
			}
			if (source.get("postcode") != null) {
				job.setZip(source.get("postcode").toString());
			}
			esJobList.add(job);
		}
		System.out.println("ES Jobs Count : " + esJobList.size());
		return esJobList;
	}

	public Set<ESJob> getJobPostings(String index) throws IOException, InterruptedException {
		Set<ESJob> esJobList = new HashSet<ESJob>();
		ArrayList<SearchHit> hits = getAllRecords(index);

		for (SearchHit hit : hits) {
			Map<String, Object> source = hit.getSourceAsMap();
			ESJob job = new ESJob(hit.getId());
			if (source.get("postingDate") != null) {
				job.setImportdate(source.get("postingDate").toString());
			}
			if (source.get("title") != null) {
				job.setTitle(source.get("title").toString());
			}
			if (source.get("company") != null) {
				job.setCompany(source.get("company").toString());
			}
			if (source.get("city") != null) {
				job.setCity(source.get("city").toString());
			}
			if (source.get("postcode") != null) {
				job.setZip(source.get("postcode").toString());
			}
			esJobList.add(job);
		}
		System.out.println("ES Jobs Count : " + esJobList.size());
		return esJobList;
	}

	public Map<String, Integer> getNationalPayForRoles() {
		{
			Map<String, Integer> roleMap = new HashMap<String, Integer>();

			SearchSourceBuilder builder = new SearchSourceBuilder();
			MatchAllQueryBuilder matchAllQuery = QueryBuilders.matchAllQuery();

			builder.fetchSource(new String[] { "title", "averagePay" }, null);
			builder.query(matchAllQuery);
			builder.size(5000); // max is 10000

			SearchRequest searchRequest = new SearchRequest();
			searchRequest.indices("roles_index_03_05_2023").source(builder);
			try {
				ArrayList<SearchHit> searchHits = executeSearch(searchRequest, 3);
				for (SearchHit searchHit : searchHits) {
					String roleTitle = (String) searchHit.getSourceAsMap().get("title");
					Integer avgPay = (Integer) searchHit.getSourceAsMap().get("averagePay");
					roleMap.put(roleTitle, avgPay);
				}
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}

			// shutdown();
			return roleMap;
		}

	}
}


and i also have existing Nodejs code

import { S3Client, PutObjectCommand, ListObjectsV2Command, DeleteObjectCommand } from '@aws-sdk/client-s3';
import aws4 from 'aws4';
import { create } from 'xmlbuilder';
import dotenv from 'dotenv';
import readline from 'readline';

dotenv.config({ path: '.env.local' });
const region = process.env.region;
const credentials = {
  accessKeyId: process.env.accessKeyId,
  secretAccessKey: process.env.secretAccessKey,
};
const S3_BUCKET = process.env.S3_BUCKET;
const S3_PUBLIC_PATH = process.env.S3_PUBLIC_PATH;
const OPEN_SEARCH_URL = process.env.OPEN_SEARCH_URL;
const s3Client = new S3Client({
  region: region,
  credentials,
});
const INDEXNOW_API_KEY = process.env.INDEXNOW_API_KEY; // Add your API key in .env.local
const SEARCH_ENGINES = [
  "https://api.indexnow.org/indexnow",
  "https://www.bing.com/indexnow",
  "https://searchadvisor.naver.com/indexnow",
  "https://search.seznam.cz/indexnow",
  "https://yandex.com/indexnow",
  "https://indexnow.yep.com/indexnow"
];
const getCurrentDateTime = () => {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');
  const hours = String(now.getHours()).padStart(2, '0');
  const minutes = String(now.getMinutes()).padStart(2, '0');
  const seconds = String(now.getSeconds()).padStart(2, '0');

  return ${year}-${month}-${day}T${hours}:${minutes}:${seconds}+00:00;
};


const uploadToS3 = async (fileName, fileContent) => {
  console.log(Uploading file: ${fileName} to S3...);
  const params = {
    Bucket: S3_BUCKET,
    Key: ${S3_PUBLIC_PATH}${fileName},
    Body: fileContent,
    ContentType: 'application/xml',
    ACL: 'public-read',
  };

  try {
    const command = new PutObjectCommand(params);
    await s3Client.send(command);
    console.log(File uploaded successfully: ${fileName});
    if (fileName !== 'sitemap_Alljobs.xml') {
      await submitToIndexNow(https://www.jobtrees.com/api/sitemap/${fileName});
    }
  } catch (error) {
    console.error(Error uploading file to S3: ${error.message});
  }
};

const fetchTotalPages = async (indexName, pageSize) => {
  try {
    const requestBody = { query: { match_all: {} } };
    const countRequest = {
      host: OPEN_SEARCH_URL,
      path: /${indexName}/_count,
      service: 'es',
      region,
      method: 'POST',
      body: JSON.stringify(requestBody),
      headers: { 'Content-Type': 'application/json' },
    };

    aws4.sign(countRequest, credentials);
    const countResponse = await fetch(https://${countRequest.host}${countRequest.path}, {
      method: countRequest.method,
      headers: countRequest.headers,
      body: countRequest.body,
    });
    const countResponseBody = await countResponse.json();
    const totalCount = countResponseBody.count;

    return Math.ceil(totalCount / pageSize);
  } catch (error) {
    console.error(Error fetching total pages for index ${indexName}:, error.message);
    return 0;
  }
};

const generateSitemapXml = async (indexName, pageNumber, pageSize) => {
  try {
    console.log(Generating sitemap for index: ${indexName}, page: ${pageNumber});
    const searchRequestBody = {
      query: { match_all: {} },
      from: (pageNumber - 1) * pageSize,
      size: pageSize,
      _source: ["id", "postingDate", "updatedDate", "status"]
    };

    const searchRequest = {
      host: OPEN_SEARCH_URL,
      path: /${indexName}/_search,
      service: 'es',
      region,
      method: 'POST',
      body: JSON.stringify(searchRequestBody),
      headers: { 'Content-Type': 'application/json' },
    };

    aws4.sign(searchRequest, credentials);
    const searchResponse = await fetch(https://${searchRequest.host}${searchRequest.path}, {
      method: searchRequest.method,
      headers: searchRequest.headers,
      body: searchRequest.body,
    });

    const searchResponseBody = await searchResponse.json();
    const jobPostings = (searchResponseBody.hits && searchResponseBody.hits.hits) || [];

    // Filter only active jobs
    const activePostings = jobPostings.filter((posting) => {
      const status = posting._source.status || "Active"; // Default to Active
      return status === "Active";
    });

    if (activePostings.length > 0) {
      const root = create('urlset').att('xmlns', 'http://www.sitemaps.org/schemas/sitemap/0.9');
      activePostings.forEach((posting) => {
        const locValue = https://www.jobtrees.com/postid/${posting._source.id};
        const lastModified = posting._source.updatedDate || posting._source.postingDate || new Date().toISOString();

        root.ele('url').ele('loc', locValue).up()
          .ele('lastmod', new Date(lastModified).toISOString()).up()
          .ele('changefreq', 'daily').up()
          .ele('priority', 1.0).up();
      });

      const fileName = ${indexName}_${pageNumber}.xml;
      await uploadToS3(fileName, root.end({ pretty: true }));
      console.log(Sitemap ${fileName} generated and uploaded.);
    } else {
      console.log(No active job postings found for page ${pageNumber} of index ${indexName}.);
    }
  } catch (error) {
    console.error(Error generating sitemap for page ${pageNumber} of index ${indexName}:, error.message);
  }
};

const deleteOutdatedFiles = async (validFiles) => {
  const existingFiles = await getExistingSitemapFiles();
  const filesToDelete = existingFiles.filter((file) => {
    return !validFiles.includes(file) &&
      !['public/sitemap_Alljobs.xml', 'public/sitemap_index_counts.xml'].includes(file);
  });

  for (const file of filesToDelete) {
    try {
      const deleteParams = {
        Bucket: S3_BUCKET,
        Key: file,
      };

      await s3Client.send(new DeleteObjectCommand(deleteParams));
      console.log(Deleted outdated file: ${file});
    } catch (error) {
      console.error(Error deleting file ${file}:, error.message);
    }
  }
};

const generateSitemapsAndCleanup = async (indices) => {
  const pageSize = 5000;
  const validFiles = [];

  for (const indexName of indices) {
    const totalPages = await fetchTotalPages(indexName, pageSize);

    console.log(Total pages for ${indexName}: ${totalPages});
    for (let page = 1; page <= totalPages; page++) {
      const fileName = ${indexName}_${page}.xml;
      validFiles.push(${S3_PUBLIC_PATH}${fileName});
      // await generateSitemapXml(indexName, page, pageSize);
    }
  }
  await deleteOutdatedFiles(validFiles);
};

const getExistingSitemapFiles = async () => {
  let isTruncated = true;
  let continuationToken = null;
  const files = [];

  try {
    while (isTruncated) {
      const listParams = {
        Bucket: S3_BUCKET,
        Prefix: S3_PUBLIC_PATH,
        ContinuationToken: continuationToken,
      };

      const response = await s3Client.send(new ListObjectsV2Command(listParams));
      if (response.Contents) {
        files.push(...response.Contents.map((item) => item.Key));
      }

      isTruncated = response.IsTruncated;
      continuationToken = response.NextContinuationToken;
    }

    return files.filter((file) => file.endsWith('.xml'));
  } catch (error) {
    console.error('Error fetching files from S3:', error.message);
    return [];
  }
};

const generateMainSitemapXml = async (existingFiles) => {
  const root = create('urlset').att('xmlns', 'http://www.sitemaps.org/schemas/sitemap/0.9');
  const excludedFiles = [
    'public/sitemap_Alljobs.xml',
    'public/sitemap_index_counts.xml'
  ];

  const filteredFiles = existingFiles.filter(file => !excludedFiles.includes(file));

  filteredFiles.sort((a, b) => {
    const numA = parseInt(a.replace(/\D/g, ''), 10);
    const numB = parseInt(b.replace(/\D/g, ''), 10);
    return numA - numB;
  });

  filteredFiles.forEach((file) => {
    // Remove "public/" prefix to construct the correct URL
    const cleanedFile = file.replace(/^public\//, '');
    const url = https://www.jobtrees.com/api/sitemap/${cleanedFile};
    root.ele('url').ele('loc', url).up()
      .ele('lastmod', new Date().toISOString()).up()
      .ele('changefreq', 'daily').up()
      .ele('priority', 1.0).up();
  });

  const xmlString = root.end({ pretty: true });
  const fileName = 'sitemap_Alljobs.xml';

  await uploadToS3(fileName, xmlString);
  console.log('Main sitemap index generated and uploaded.');
};

const generateMissedSitemaps = async (indices) => {
  const pageSize = 5000;
  console.log('missed sitemaps generating')
  for (const indexName of indices) {
    try {
      // Fetch existing sitemap files from S3
      const existingFiles = await getExistingSitemapFiles();
      const existingPages = existingFiles
        .filter((file) => file.startsWith(${S3_PUBLIC_PATH}${indexName}_))
        .map((file) => {
          const match = file.match(/_(\d+)\.xml$/);
          return match ? parseInt(match[1], 10) : null;
        })
        .filter((num) => num !== null);

      // Get total pages based on total documents in the index
      const totalPages = await fetchTotalPages(indexName, pageSize);

      // Identify missing pages
      const missingPages = [];
      for (let i = 1; i <= totalPages; i++) {
        if (!existingPages.includes(i)) {
          missingPages.push(i);
        }
      }

      if (missingPages.length > 0) {
        console.log(Generating missed sitemaps for index ${indexName}:, missingPages);

        // Iterate only through the missingPages array
        for (const page of missingPages) {
          console.log(Processing missing page: ${page}); // Debug log
          await generateSitemapXml(indexName, page, pageSize, totalPages);
        }

      } else {
        console.log(No missing sitemaps found for index ${indexName}.);
      }
    } catch (error) {
      console.error(Error processing index ${indexName}:, error.message);
    }
  }
  const existingFiles = await getExistingSitemapFiles();
  await generateMainSitemapXml(existingFiles);
};

const submitToIndexNow = async (sitemapUrl) => {
  if (!INDEXNOW_API_KEY) {
    console.error("❌ INDEXNOW_API_KEY is missing. Add it to your environment variables.");
    return;
  }

  try {
    console.log(\n🚀 Submitting ${sitemapUrl} to all IndexNow search engines...\n);

    const requestBody = {
      host: "www.jobtrees.com",
      key: INDEXNOW_API_KEY,
      keyLocation: https://www.jobtrees.com/${INDEXNOW_API_KEY}.txt,
      urlList: [sitemapUrl],
    };

    // console.log("📤 Request Details:");
    // console.log("🔹 Body:", JSON.stringify(requestBody, null, 2));

    // Send requests in parallel to all search engines
    const requests = SEARCH_ENGINES.map(async (endpoint) => {
      // console.log(\n🌍 Submitting to: ${endpoint});

      try {
        const response = await fetch(endpoint, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(requestBody),
        });

        // console.log("\n📥 Response Details:");
        // console.log("🔹 Search Engine:", endpoint);
        // console.log("🔹 Status:", response.status, response.statusText);
        // console.log("🔹 Headers:", Object.fromEntries(response.headers.entries()));

        const responseText = await response.text();
        // console.log("🔹 Response Body:", responseText || "[Empty Response]");

        return { endpoint, success: response.ok };
      } catch (error) {
        console.error(❌ Error submitting to ${endpoint}:, error.message);
        return { endpoint, success: false };
      }
    });

    // Wait for all requests to complete
    const results = await Promise.all(requests);

    // Summary of successful and failed submissions
    const successfulSubmissions = results.filter((r) => r.success);
    console.log(\n✅ Successfully submitted to ${successfulSubmissions.length}/${SEARCH_ENGINES.length} search engines.);

    if (successfulSubmissions.length !== SEARCH_ENGINES.length) {
      console.warn("⚠️ Some search engines failed to receive the submission.");
    }
  } catch (error) {
    console.error(🚨 Unexpected error submitting to IndexNow:, error);
  }
};

const generateAllSitemaps = async (indices) => {
  const pageSize = 5000;
  for (const indexName of indices) {
    const totalPages = await fetchTotalPages(indexName, pageSize);
    console.log(Total pages for ${indexName}: ${totalPages});

    for (let page = 1; page <= totalPages; page++) {
      await generateSitemapXml(indexName, page, pageSize);
    }
    await generateMissedSitemaps(indices)
  }
};

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

(async () => {
  try {
    const indices = [
      'jobtrees_postings',
      'indeed_jobs_postings',
      'big_job_site_postings',
      'perengo_postings',
      'adzuna_postings',
    ];
    // await generateSitemapsAndCleanup(indices)
    await generateAllSitemaps(indices);
  } catch (error) {
    console.error('Error during the process:', error.message);
  } finally {
    rl.close();
  }
})();

i need to inccoporate nodejs code into JAVA code so all the things works together in JAVA code script need to get rid of node js script