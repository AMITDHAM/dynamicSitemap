const { S3Client, PutObjectCommand, ListObjectsV2Command, DeleteObjectsCommand, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const aws4 = require('aws4');
const readline = require('readline');
const { create } = require('xmlbuilder');
require('dotenv').config({ path: '.env.local' });
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

    if (jobPostings.length > 0) {
      const root = create('urlset').att('xmlns', 'http://www.sitemaps.org/schemas/sitemap/0.9');
      jobPostings.forEach((posting) => {
        const locValue = `https://www.jobtrees.com/postid/${posting._source.id}`;
        root.ele('url').ele('loc', locValue).up()
          .ele('lastmod', getCurrentDateTime()).up()
          .ele('changefreq', 'daily').up()
          .ele('priority', 1.0).up();
      });

      const fileName = `${indexName}_${pageNumber}.xml`;
      await uploadToS3(fileName, root.end({ pretty: true }));
      console.log(`Sitemap ${fileName} generated and uploaded.`);
    } else {
      console.log(`No job postings found for page ${pageNumber} of index ${indexName}.`);
    }
  } catch (error) {
    console.error(`Error generating sitemap for page ${pageNumber} of index ${indexName}:`, error.message);
  }
};

const generateSitemapXmlSerial = async (indexName, startIndex) => {
  let pageNumber = startIndex;
  let fileCounter = pageNumber - 1;
  const pageSize = 5000;

  try {
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

const parsePageInput = (input, totalPages) => {
  const pages = new Set();

  const parts = input.split(',');
  for (const part of parts) {
    if (part.includes('-')) {
      const [start, end] = part.split('-').map(Number);
      for (let i = Math.max(1, start); i <= Math.min(end, totalPages); i++) {
        pages.add(i);
      }
    } else {
      const page = parseInt(part, 10);
      if (page >= 1 && page <= totalPages) {
        pages.add(page);
      }
    }
  }

  return Array.from(pages).sort((a, b) => a - b);
};

const generateAllSitemaps = async (indices) => {
  const pageSize = 5000;

  for (const indexName of indices) {
    const totalPages = await fetchTotalPages(indexName, pageSize);
    console.log(`Total pages for ${indexName}: ${totalPages}`);

    const userInput = await new Promise((resolve) => {
      rl.question(
        `Enter page numbers for ${indexName} (e.g., "1", "1-10", "5,10,20"): `,
        (input) => resolve(input)
      );
    });

    const pages = userInput === '0'
      ? Array.from({ length: totalPages }, (_, i) => i + 1) // All pages
      : parsePageInput(userInput, totalPages);

    for (const page of pages) {
      await generateSitemapXml(indexName, page, pageSize);
    }
  }
};

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

const promptUserForChoice = () => {
  return new Promise((resolve) => {
    rl.question(
      'Choose an option:\n1. Generate all sitemaps (serial wise)\n2. Generate all sitemaps (range or specifics after 0)\nYour choice: ',
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
    // const choice = await promptUserForChoice();
    const indices = [
      'indeed_jobs_postings',
    ];

    // if (choice === 1) {
    //   const startingPage = await promptUserForStartingPage();
    //   console.log(`Generating sitemaps starting from page ${startingPage}...`);
    //   // if (startingPage === 0) {
    //   //   console.log('Deleting existing sitemaps...');
    //   //   // await clearExistingSitemaps();
    //   // }
    //   const startIndex = startingPage === 0 ? 1 : startingPage;
    //   for (const indexName of indices) {
    //     await generateSitemapXmlSerial(indexName, startIndex);
    //   }
    // } else if (choice === 2) {
    //   console.log('Generating all sitemaps...');
      await generateAllSitemaps(indices);
    // } else {
    //   console.log('Invalid choice. Exiting...');
    // }
  } catch (error) {
    console.error('Error during the process:', error.message);
  } finally {
    rl.close();
  }
})();
