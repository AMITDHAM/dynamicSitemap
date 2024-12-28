const { S3Client, PutObjectCommand, ListObjectsV2Command, DeleteObjectsCommand } = require('@aws-sdk/client-s3');
const aws4 = require('aws4');
const readline = require('readline');
const { create } = require('xmlbuilder');
const region = 'us-east-1';
const credentials = {
  accessKeyId: 'AKIAW6VFAJMZXJ7J4TXL',
  secretAccessKey: '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',
};
const s3Client = new S3Client({
  region: region,
  credentials,
});
const S3_BUCKET = 'jobtrees-media-assets-staging';
const S3_PUBLIC_PATH = 'public/';
const OPEN_SEARCH_URL = 'search-stageelatic-fegphos2kqdtkacicwzq3izmpq.us-east-1.es.amazonaws.com';

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
  try {
    const listParams = {
      Bucket: S3_BUCKET,
      Prefix: S3_PUBLIC_PATH,
    };

    const { Contents } = await s3Client.send(new ListObjectsV2Command(listParams));
    const files = Contents ? Contents.map((item) => item.Key) : [];
    return files.filter((file) => file.endsWith('.xml'));
  } catch (error) {
    console.error('Error fetching existing sitemap files:', error.message);
    return [];
  }
};

const generateSitemapXml = async (indexName, startIndex) => {
  let pageNumber = startIndex;
  const pageSize = 5000;
  let hasMoreResults = true;

  try {
    while (hasMoreResults) {
      console.log(`Fetching page ${pageNumber} for index ${indexName}...`);

      const requestBody = {
        query: { match_all: {} },
        from: (pageNumber - 1) * pageSize,
        size: pageSize,
      };

      const request = {
        host: OPEN_SEARCH_URL,
        path: `/${indexName}/_search`,
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
      const jobPostings = responseBody.hits && responseBody.hits.hits || [];
      if (jobPostings.length === 0) {
        console.log(`No job postings found for page ${pageNumber} in index ${indexName}. Ending generation.`);
        hasMoreResults = false;
        break;
      }

      const root = create('urlset').att('xmlns', 'http://www.sitemaps.org/schemas/sitemap/0.9');

      jobPostings.forEach((posting) => {
        const postingId = posting._source.id;
        const locValue = `https://www.jobtrees.com/postid/${postingId}`;

        root.ele('url').ele('loc', locValue).up()
          .ele('lastmod', getCurrentDateTime()).up()
          .ele('changefreq', 'daily').up()
          .ele('priority', 1.0).up();
      });

      const xmlString = root.end({ pretty: true });
      const fileName = `sitemap_jobpostings${pageNumber}.xml`;

      await uploadToS3(fileName, xmlString);
      console.log(`Sitemap ${fileName} generated and uploaded.`);
      await delay(0);
      if (jobPostings.length < pageSize) {
        console.log(`Fewer than ${pageSize} job postings found on page ${pageNumber}. Ending sitemap generation.`);
        hasMoreResults = false;
      }

      pageNumber++;
    }

    return `All sitemaps for index ${indexName} generated successfully.`;
  } catch (error) {
    console.error(`Error generating sitemap for index ${indexName}:`, error.message);
    return null;
  }
};

const generateIndexCountSitemap = async (indices) => {
  const root = create('urlset').att('xmlns', 'http://www.sitemaps.org/schemas/sitemap/0.9');
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
      let totalCount = responseBody.count;
      if (!totalCount) {
        throw new Error(`Unable to fetch count from index ${indexName}`);
      }

      const locValue = `${indexName}_${totalCount}`;

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
  console.log('Index count sitemap generated and uploaded.');
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
    const url = `https://www.jobtrees.com/api/sitemap/${file}`;
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
          console.log('Invalid input. Starting from page 0 and deleting existing sitemaps.');
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
    const choice = 2;
    const startingPage = 0; 
    if (choice === 1) {
      console.log(`Generating sitemaps starting from page ${startingPage}...`);
      if (startingPage === 0) {
        console.log('Deleting existing sitemaps...');
        // await clearExistingSitemaps();
      }

      const indexnames = [
        'linkup_postings',
        'adzuna_postings',
        'indeed_jobs_postings',
        'big_job_site_postings',
      ];

      await generateIndexCountSitemap(indexnames);

      let startIndex = startingPage;
      for (const indexName of indexnames) {
        await generateSitemapXml(indexName, startIndex);
      }

    } else if (choice === 2) {
      const existingFiles = await getExistingSitemapFiles();
      await generateMainSitemapXml(existingFiles);
      console.log('Main sitemap created.');

    } else {
      console.log('Invalid choice. Exiting...');
    }
  } catch (error) {
    console.error('Error during the process:', error.message);
  } finally {
    rl.close();
    process.exit(0);
  }
})();
