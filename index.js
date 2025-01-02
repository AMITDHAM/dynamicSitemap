const { S3Client, PutObjectCommand, ListObjectsV2Command, DeleteObjectsCommand } = require('@aws-sdk/client-s3');
const aws4 = require('aws4');
const readline = require('readline');
const { create } = require('xmlbuilder');
const region = 'us-east-1';
const credentials = {
  accessKeyId: 'AKIA4SWDP73D3EEMFZ4F',
  secretAccessKey: '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',
};
const S3_BUCKET = 'jobtrees-media-assets';
const S3_PUBLIC_PATH = 'public/';
const OPEN_SEARCH_URL = 'search-jobtrees-iqdimaxupmniwiygtkt7nxj3ku.us-east-1.es.amazonaws.com';
const s3Client = new S3Client({
  region: region,
  credentials,
});
const getCurrentDateTime = () => new Date().toISOString();
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const clearExistingSitemaps = async () => {
  try {
    const listParams = {
      Bucket: S3_BUCKET,
      Prefix: S3_PUBLIC_PATH,
    };

    const { Contents } = await s3Client.send(new ListObjectsV2Command(listParams));
    if (Contents && Contents.length > 0) {
      const deleteParams = {
        Bucket: S3_BUCKET,
        Delete: {
          Objects: Contents.map((item) => ({ Key: item.Key })),
          Quiet: true,
        },
      };

      await s3Client.send(new DeleteObjectsCommand(deleteParams));
      console.log('Existing sitemap files deleted successfully.');
    } else {
      console.log('No existing sitemap files found in S3.');
    }
  } catch (error) {
    console.error('Error clearing existing sitemap files:', error.message);
  }
};

const getExistingSitemapFiles = async () => {
  let isTruncated = true; // Indicates if more pages are available
  let continuationToken = null; // Token for the next set of results
  const files = [];

  try {
    while (isTruncated) {
      const listParams = {
        Bucket: S3_BUCKET,
        Prefix: S3_PUBLIC_PATH,
        ContinuationToken: continuationToken, // Set continuation token for pagination
      };

      const response = await s3Client.send(new ListObjectsV2Command(listParams));
      if (response.Contents) {
        files.push(...response.Contents.map(item => item.Key));
      }

      // Check if more files are available
      isTruncated = response.IsTruncated;
      continuationToken = response.NextContinuationToken;
    }

    return files.filter(file => file.endsWith('.xml'));
  } catch (error) {
    console.error('Error fetching files from S3:', error.message);
    return [];
  }
};

const generateSitemapXml = async (indexName, startIndex) => {
  let pageNumber = startIndex; // Start from the specified page
  let fileCounter = pageNumber - 1; // Ensure unique file naming
  const pageSize = 5000;

  try {
      // Fetch the total count to calculate the expected number of pages
      const requestBody = { query: { match_all: {} } };
      const countRequest = {
          host: OPEN_SEARCH_URL,
          path: `/${indexName}/_count`,
          service: 'es',
          region: region,
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

      const totalPages = Math.ceil(totalCount / pageSize);
      console.log(`Total pages for index ${indexName}: ${totalPages}`);

      // Generate sitemaps for all pages starting from the given index
      while (pageNumber <= totalPages) {
          console.log(`Fetching page ${pageNumber} for index ${indexName}...`);
          const searchRequestBody = {
              query: { match_all: {} },
              from: (pageNumber - 1) * pageSize,
              size: pageSize,
          };

          const searchRequest = {
              host: OPEN_SEARCH_URL,
              path: `/${indexName}/_search`,
              service: 'es',
              region: region,
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

          if (jobPostings.length === 0) {
              console.log(`No job postings found for page ${pageNumber}.`);
          } else {
              const root = create('urlset').att('xmlns', 'http://www.sitemaps.org/schemas/sitemap/0.9');
              jobPostings.forEach((posting) => {
                  const locValue = `https://www.jobtrees.com/postid/${posting._source.id}`;
                  root.ele('url').ele('loc', locValue).up()
                      .ele('lastmod', getCurrentDateTime()).up()
                      .ele('changefreq', 'daily').up()
                      .ele('priority', 1.0).up();
              });

              const fileName = `${indexName}_${fileCounter}.xml`;
              await uploadToS3(fileName, root.end({ pretty: true }));
              console.log(`Sitemap ${fileName} generated and uploaded.`);
          }

          fileCounter++;
          pageNumber++;
      }
  } catch (error) {
      console.error(`Error generating sitemap for index ${indexName}:`, error.message);
  }
};

const generateIndexCountSitemap = async (indices) => {
  const root = create('urlset').att('xmlns', 'http://www.sitemaps.org/schemas/sitemap/0.9');
  const pageSize = 5000; // Number of postings per sitemap

  for (const indexName of indices) {
    try {
      const requestBody = {
        query: { match_all: {} },
      };

      const request = {
        host: OPEN_SEARCH_URL,
        path: `/${indexName}/_count`,
        service: 'es',
        region: region,
        method: 'POST',
        body: JSON.stringify(requestBody),
        headers: {
          'Content-Type': 'application/json',
        },
      };

      aws4.sign(request, {
        accessKeyId: credentials.accessKeyId,
        secretAccessKey: credentials.secretAccessKey,
      });

      const response = await fetch(`https://${request.host}${request.path}`, {
        method: request.method,
        headers: request.headers,
        body: request.body,
      });
      const responseBody = await response.json();
      const totalCount = responseBody.count;

      if (!totalCount) {
        throw new Error(`Unable to fetch count from index ${indexName}`);
      }

      const sitemapCount = Math.ceil(totalCount / pageSize);

      // Include the summary in the XML structure
      const locValue = `Index: ${indexName}, Total Postings: ${totalCount}, Sitemaps to Generate: ${sitemapCount}`;

      root.ele('url').ele('loc', locValue).up()
        .ele('lastmod', getCurrentDateTime()).up()
        .ele('changefreq', 'daily').up()
        .ele('priority', 1.0).up();
    } catch (error) {
      console.error(`Error fetching count for index ${indexName}:`, error.message);
    }
  }

  const xmlString = root.end({ pretty: true });
  const fileName = 'sitemap_index_counts.xml';

  await uploadToS3(fileName, xmlString);
  const fileUrl = `https://www.jobtrees.com/api/sitemap/${fileName}`;
  console.log(`Index count sitemap generated and uploaded. Accessible at: ${fileUrl}`);
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
  } catch (error) {
    console.error(`Error uploading file to S3: ${error.message}`);
  }
};

const generateMainSitemapXml = async (existingFiles) => {
  const root = create('sitemapindex').att('xmlns', 'http://www.sitemaps.org/schemas/sitemap/0.9');
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
      .ele('lastmod', getCurrentDateTime()).up()
      .ele('changefreq', 'daily').up()
      .ele('priority', 1.0).up();
  });

  const xmlString = root.end({ pretty: true });
  const fileName = 'sitemap_Alljobs.xml';

  await uploadToS3(fileName, xmlString);
  console.log('Main sitemap index generated and uploaded.');
};

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

const promptUserForChoice = () => {
  return new Promise((resolve) => {
    rl.question(
      'Choose an option:\n1. Generate all sitemaps\n2. Generate only the alljobs sitemap index from existing files\nYour choice: ',
      (choice) => {
        resolve(parseInt(choice, 10));
      }
    );
  });
};


const promptUserForStartingPage = () => {
  return new Promise((resolve) => {
    rl.question(
      'Enter the starting page number for generating sitemaps (0 to delete existing sitemaps and start from the beginning): ',
      (input) => {
        const startingPage = parseInt(input, 10);
        if (isNaN(startingPage) || startingPage < 0) {
          console.log('Invalid input. Starting from page 1 and deleting existing sitemaps.');
          resolve(0);
        } else {
          resolve(startingPage);
        }
      }
    );
  });
};

(async () => {
  try {
    const choice = await promptUserForChoice();
    if (choice === 1) {
      const startingPage = await promptUserForStartingPage();

      console.log(`Generating sitemaps starting from page ${startingPage}...`);
      if (startingPage === 0) {
        console.log('Deleting existing sitemaps...');
        await clearExistingSitemaps();
      }
      const startIndex = startingPage === 0 ? 1 : startingPage;
      const indexnames = [
        'linkup_postings',
        'adzuna_postings',
        'indeed_jobs_postings',
        'big_job_site_postings',
      ];
      for (const indexName of indexnames) {
        await generateSitemapXml(indexName, startIndex);
      }
    } else if (choice === 2) {
      const indexnamess = [
        'linkup_postings',
        'adzuna_postings',
        'indeed_jobs_postings',
        'big_job_site_postings',
      ];
      await generateIndexCountSitemap(indexnamess)
      const existingFiles = await getExistingSitemapFiles();
      await generateMainSitemapXml(existingFiles);
    } else {
      console.log('Invalid choice. Exiting...');
    }
  } catch (error) {
    console.error('Error during the process:', error.message);
  } finally {
    rl.close();
  }
})();
