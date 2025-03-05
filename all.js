import { S3Client, PutObjectCommand, ListObjectsV2Command, DeleteObjectCommand } from '@aws-sdk/client-s3';
import aws4 from 'aws4';
import { create } from 'xmlbuilder';
import dotenv from 'dotenv';
import Upload from '@aws-sdk/lib-storage';

dotenv.config({ path: '.env.local' });
const region = process.env.region;
const credentials = {
  accessKeyId: process.env.accessKeyId,
  secretAccessKey: process.env.secretAccessKey,
};
const S3_BUCKET = process.env.S3_BUCKET;
const S3_PUBLIC_PATH = process.env.S3_PUBLIC_PATH;
const OPEN_SEARCH_URL = process.env.OPEN_SEARCH_URL;

const s3Client = new S3Client({ region, credentials });
const escapeXml = (str) => {
  return str.replace(/[<&>]/g, (match) => {
    switch (match) {
      case '&': return '&amp;';
      case '<': return '&lt;';
      case '>': return '&gt;';
      default: return match;
    }
  });
};
const uploadToS3 = async (fileName, fileContent) => {
  try {
    console.log(`Uploading: ${fileName}...`);
    const upload = new Upload({
      client: s3Client,
      params: {
        Bucket: S3_BUCKET,
        Key: `${S3_PUBLIC_PATH}${fileName}`,
        Body: fileContent,
        ContentType: 'application/xml',
        ACL: 'public-read',
      },
    });
    await upload.done();
    console.log(`Uploaded: ${fileName}`);
  } catch (error) {
    console.error(`Error uploading ${fileName}:`, error.message);
  }
};

const fetchTotalPages = async (indexName, pageSize) => {
  try {
    console.log(`Fetching total pages for ${indexName}...`);
    const request = {
      host: OPEN_SEARCH_URL,
      path: `/${indexName}/_count`,
      service: 'es',
      region,
      method: 'POST',
      body: JSON.stringify({ query: { match_all: {} } }),
      headers: { 'Content-Type': 'application/json' },
    };
    aws4.sign(request, credentials);
    const response = await fetch(`https://${request.host}${request.path}`, {
      method: request.method,
      headers: request.headers,
      body: request.body,
    });
    const data = await response.json();
    const totalPages = Math.ceil(data.count / pageSize);
    console.log(`Total pages for ${indexName}: ${totalPages}`);
    return totalPages;
  } catch (error) {
    console.error(`Error fetching page count for ${indexName}:`, error.message);
    return 0;
  }
};

const generateSitemapXml = async (indexName, pageNumber, pageSize) => {
  try {
    console.log(`Generating sitemap for ${indexName} page ${pageNumber}...`);

    const searchRequest = {
      host: OPEN_SEARCH_URL,
      path: `/${indexName}/_search`,
      service: 'es',
      region,
      method: 'POST',
      body: JSON.stringify({ query: { match_all: {} }, from: (pageNumber - 1) * pageSize, size: pageSize, _source: ["id"] }),
      headers: { 'Content-Type': 'application/json' },
    };

    aws4.sign(searchRequest, credentials);

    const searchResponse = await fetch(`https://${searchRequest.host}${searchRequest.path}`, {
      method: searchRequest.method,
      headers: searchRequest.headers,
      body: searchRequest.body,
    });

    const data = await searchResponse.json();
    const jobPostings = data.hits?.hits || [];

    if (!jobPostings.length) {
      console.log(`No postings found for page ${pageNumber}`);
      return;
    }

    // Ensure correct node creation
    const root = create({ version: '1.0' }).ele('urlset', { xmlns: 'http://www.sitemaps.org/schemas/sitemap/0.9' });

    jobPostings.forEach(posting => {
      const urlNode = root.ele('url');
      urlNode.ele('loc').txt(`https://www.jobtrees.com/postid/${posting._source.id}`).up();

      const lastmod = new Date().toISOString();
      urlNode.ele('lastmod').txt(lastmod).up();

      // Ensure changefreq and priority are properly formatted
      urlNode.ele('changefreq').txt('daily').up();
      urlNode.ele('priority').txt('1.0').up();
    });

    const xmlString = root.end({ prettyPrint: true });
    await uploadToS3(`${indexName}_${pageNumber}.xml`, xmlString);
    console.log(`Sitemap for ${indexName} page ${pageNumber} generated and uploaded.`);
  } catch (error) {
    console.error(`Error generating sitemap for ${indexName} page ${pageNumber}:`, error.message);
  }
};



const deleteOutdatedFiles = async (validFiles) => {
  try {
    console.log('Deleting outdated files...');
    const existingFiles = await getExistingSitemapFiles();
    const filesToDelete = existingFiles.filter(file => !validFiles.has(file));
    if (!filesToDelete.length) {
      console.log('No outdated files to delete');
      return;
    }

    await s3Client.send(new DeleteObjectsCommand({
      Bucket: S3_BUCKET,
      Delete: { Objects: filesToDelete.map(Key => ({ Key })) },
    }));
    console.log(`Deleted outdated files: ${filesToDelete.length}`);
  } catch (error) {
    console.error('Error deleting outdated files:', error.message);
  }
};

const getExistingSitemapFiles = async () => {
  const files = new Set();
  let continuationToken;
  do {
    const { Contents, IsTruncated, NextContinuationToken } = await s3Client.send(new ListObjectsV2Command({
      Bucket: S3_BUCKET,
      Prefix: S3_PUBLIC_PATH,
      ContinuationToken: continuationToken,
    }));
    if (Contents) {
      Contents.forEach(({ Key }) => files.add(Key));
    }
    continuationToken = IsTruncated ? NextContinuationToken : null;
  } while (continuationToken);

  return Array.from(files); // Ensure it's an array
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

const generateIndexCountSitemap = async (indices) => {
  const root = create('urlset').att('xmlns', 'http://www.sitemaps.org/schemas/sitemap/0.9');
  const pageSize = 5000;

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

      const locValue = `Index: ${indexName}, Total Postings: ${totalCount}, Sitemaps to Generate: ${sitemapCount}`;

      root.ele('entry').ele('info', locValue).up()
        .ele('lastmod', new Date().toISOString()).up()
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

const generateMissedSitemaps = async (indices) => {
  const pageSize = 5000;

  for (const indexName of indices) {
    try {
      const existingFiles = await getExistingSitemapFiles();
      const existingPages = existingFiles
        .filter((file) => file.startsWith(`${S3_PUBLIC_PATH}${indexName}_`))
        .map((file) => {
          const match = file.match(/_(\d+)\.xml$/);
          return match ? parseInt(match[1], 10) : null;
        })
        .filter((num) => num !== null);

      const totalPages = await fetchTotalPages(indexName, pageSize);
      const missingPages = [];

      for (let i = 1; i <= totalPages; i++) {
        if (!existingPages.includes(i)) {
          missingPages.push(i);
        }
      }

      if (missingPages.length > 0) {
        console.log(`Generating missed sitemaps for index ${indexName}:`, missingPages);

        for (const page of missingPages) {
          console.log(`Processing missing page: ${page}`);
          await generateSitemapXml(indexName, page, pageSize);
        }
      } else {
        console.log(`No missing sitemaps found for index ${indexName}.`);
      }
    } catch (error) {
      console.error(`Error processing index ${indexName}:`, error.message);
    }
  }
};

const generateAllSitemaps = async (indices) => {
  const startTime = Date.now();
  console.log('Starting sitemap generation...');

  try {
    // Step 1: Generate all sitemaps from page 0 to the last page for each index
    // console.log('Generating all sitemaps...');
    // const pageSize = 5000;
    // for (const indexName of indices) {
    //   const totalPages = await fetchTotalPages(indexName, pageSize);
    //   console.log(`Generating sitemaps for ${indexName}...`);

    //   for (let page = 1; page <= totalPages; page++) {
    //     console.log(`Generating sitemap for ${indexName}, page ${page}...`);
    //     await generateSitemapXml(indexName, page, pageSize);
    //   }

    //   console.log(`All sitemaps for ${indexName} generated.`);
    // }
    // await generateMissedSitemaps(indices)
    // Step 2: Get existing files and generate the index count sitemap
    const existingFiles = await getExistingSitemapFiles();
    console.log('Fetching existing sitemap files...');
    await generateIndexCountSitemap(indices);
    console.log('Index count sitemap generated.');

    // Step 3: Generate the main sitemap index
    await generateMainSitemapXml(existingFiles);
    console.log('Main sitemap index generated.');

    // Step 4: Delete outdated files from S3
    console.log('Deleting outdated sitemap files...');
    await deleteOutdatedFiles(new Set(existingFiles));
    console.log('Outdated sitemap files deleted.');

    // Final step: Calculate and print total time taken
    const endTime = Date.now();
    console.log(`All sitemaps generated successfully! Total time: ${(endTime - startTime) / 1000} seconds`);

  } catch (error) {
    console.error('Error in sitemap generation process:', error.message);
  }
};


(async () => {
  try {
    const indices = ['adzuna_postings', 'big_job_site_postings', 'indeed_jobs_postings'];
    await generateAllSitemaps(indices);
  } catch (error) {
    console.error('Error in sitemap generation process:', error.message);
  }
})();
