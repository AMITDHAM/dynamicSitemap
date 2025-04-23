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

  return `${year}-${month}-${day}T${hours}:${minutes}:${seconds}+00:00`;
};


const uploadToS3 = async (fileName, fileContent) => {
  console.log(`Uploading file: ${fileName} to S3...`);
  const params = {
    Bucket: S3_BUCKET,
    Key: `${S3_PUBLIC_PATH}${fileName}`,
    Body: fileContent,
    ContentType: 'application/xml',
    ACL: 'public-read',
  };

  try {
    const command = new PutObjectCommand(params);
    await s3Client.send(command);
    console.log(`File uploaded successfully: ${fileName}`);
    if (fileName !== 'sitemap_Alljobs.xml') {
      await submitToIndexNow(`https://www.jobtrees.com/api/sitemap/${fileName}`);
    }
  } catch (error) {
    console.error(`Error uploading file to S3: ${error.message}`);
  }
};

const fetchTotalPages = async (indexName, pageSize) => {
  try {
    const requestBody = { query: { match_all: {} } };
    const countRequest = {
      host: OPEN_SEARCH_URL,
      path: `/${indexName}/_count`,
      service: 'es',
      region,
      method: 'POST',
      body: JSON.stringify(requestBody),
      headers: { 'Content-Type': 'application/json' },
    };

    aws4.sign(countRequest, credentials);
    const countResponse = await fetch(`https://${countRequest.host}${countRequest.path}`, {
      method: countRequest.method,
      headers: countRequest.headers,
      body: countRequest.body,
    });
    const countResponseBody = await countResponse.json();
    const totalCount = countResponseBody.count;

    return Math.ceil(totalCount / pageSize);
  } catch (error) {
    console.error(`Error fetching total pages for index ${indexName}:`, error.message);
    return 0;
  }
};

const generateSitemapXml = async (indexName, pageNumber, pageSize) => {
  try {
    console.log(`Generating sitemap for index: ${indexName}, page: ${pageNumber}`);
    const searchRequestBody = {
      query: { match_all: {} },
      from: (pageNumber - 1) * pageSize,
      size: pageSize,
      _source: ["id", "postingDate", "updatedDate", "status"]
    };

    const searchRequest = {
      host: OPEN_SEARCH_URL,
      path: `/${indexName}/_search`,
      service: 'es',
      region,
      method: 'POST',
      body: JSON.stringify(searchRequestBody),
      headers: { 'Content-Type': 'application/json' },
    };

    aws4.sign(searchRequest, credentials);
    const searchResponse = await fetch(`https://${searchRequest.host}${searchRequest.path}`, {
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
        const locValue = `https://www.jobtrees.com/postid/${posting._source.id}`;
        const lastModified = posting._source.updatedDate || posting._source.postingDate || new Date().toISOString();

        root.ele('url').ele('loc', locValue).up()
          .ele('lastmod', new Date(lastModified).toISOString()).up()
          .ele('changefreq', 'daily').up()
          .ele('priority', 1.0).up();
      });

      const fileName = `${indexName}_${pageNumber}.xml`;
      await uploadToS3(fileName, root.end({ pretty: true }));
      console.log(`Sitemap ${fileName} generated and uploaded.`);
    } else {
      console.log(`No active job postings found for page ${pageNumber} of index ${indexName}.`);
    }
  } catch (error) {
    console.error(`Error generating sitemap for page ${pageNumber} of index ${indexName}:`, error.message);
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
      console.log(`Deleted outdated file: ${file}`);
    } catch (error) {
      console.error(`Error deleting file ${file}:`, error.message);
    }
  }
};

const generateSitemapsAndCleanup = async (indices) => {
  const pageSize = 5000;
  const validFiles = [];

  for (const indexName of indices) {
    const totalPages = await fetchTotalPages(indexName, pageSize);

    console.log(`Total pages for ${indexName}: ${totalPages}`);
    for (let page = 1; page <= totalPages; page++) {
      const fileName = `${indexName}_${page}.xml`;
      validFiles.push(`${S3_PUBLIC_PATH}${fileName}`);
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
    const url = `https://www.jobtrees.com/api/sitemap/${cleanedFile}`;
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
        .filter((file) => file.startsWith(`${S3_PUBLIC_PATH}${indexName}_`))
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
        console.log(`Generating missed sitemaps for index ${indexName}:`, missingPages);

        // Iterate only through the missingPages array
        for (const page of missingPages) {
          console.log(`Processing missing page: ${page}`); // Debug log
          await generateSitemapXml(indexName, page, pageSize, totalPages);
        }

      } else {
        console.log(`No missing sitemaps found for index ${indexName}.`);
      }
    } catch (error) {
      console.error(`Error processing index ${indexName}:`, error.message);
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
    console.log(`\n🚀 Submitting ${sitemapUrl} to all IndexNow search engines...\n`);

    const requestBody = {
      host: "www.jobtrees.com",
      key: INDEXNOW_API_KEY,
      keyLocation: `https://www.jobtrees.com/${INDEXNOW_API_KEY}.txt`,
      urlList: [sitemapUrl],
    };

    // console.log("📤 Request Details:");
    // console.log("🔹 Body:", JSON.stringify(requestBody, null, 2));

    // Send requests in parallel to all search engines
    const requests = SEARCH_ENGINES.map(async (endpoint) => {
      // console.log(`\n🌍 Submitting to: ${endpoint}`);

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
        console.error(`❌ Error submitting to ${endpoint}:`, error.message);
        return { endpoint, success: false };
      }
    });

    // Wait for all requests to complete
    const results = await Promise.all(requests);

    // Summary of successful and failed submissions
    const successfulSubmissions = results.filter((r) => r.success);
    console.log(`\n✅ Successfully submitted to ${successfulSubmissions.length}/${SEARCH_ENGINES.length} search engines.`);

    if (successfulSubmissions.length !== SEARCH_ENGINES.length) {
      console.warn("⚠️ Some search engines failed to receive the submission.");
    }
  } catch (error) {
    console.error(`🚨 Unexpected error submitting to IndexNow:`, error);
  }
};

const generateAllSitemaps = async (indices) => {
  const pageSize = 5000;
  for (const indexName of indices) {
    const totalPages = await fetchTotalPages(indexName, pageSize);
    console.log(`Total pages for ${indexName}: ${totalPages}`);

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
