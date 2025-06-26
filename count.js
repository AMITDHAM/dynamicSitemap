import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
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
  region,
  credentials,
});

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

const generateIndexCountSitemap = async (indices) => {
  const root = create('urlset').att('xmlns', 'http://www.sitemaps.org/schemas/sitemap/0.9');
  const pageSize = 5000;

  for (const indexName of indices) {
    try {
      const requestBody = {
        query: {
          term: {
            status: 'active',
          },
        },
      };

      const request = {
        host: OPEN_SEARCH_URL,
        path: `/${indexName}/_count`,
        service: 'es',
        region,
        method: 'POST',
        body: JSON.stringify(requestBody),
        headers: {
          'Content-Type': 'application/json',
        },
      };

      aws4.sign(request, credentials);

      const response = await fetch(`https://${request.host}${request.path}`, {
        method: request.method,
        headers: request.headers,
        body: request.body,
      });

      const responseBody = await response.json();
      const totalCount = responseBody.count;

      if (typeof totalCount !== 'number') {
        throw new Error(`Invalid count for index ${indexName}`);
      }

      const sitemapCount = Math.ceil(totalCount / pageSize);
      const locValue = `Index: ${indexName}, Active Postings: ${totalCount}, Sitemaps to Generate: ${sitemapCount}`;

      root.ele('url')
        .ele('loc', locValue).up()
        .ele('lastmod', new Date().toISOString()).up()
        .ele('changefreq', 'daily').up()
        .ele('priority', 1.0).up();

      console.log(`Index: ${indexName}`);
      console.log(`✔️ Active Job Postings: ${totalCount}`);
      console.log(`📄 Sitemaps to Generate: ${sitemapCount}`);
    } catch (error) {
      console.error(`❌ Error processing index "${indexName}":`, error.message);
    }
  }

  const xmlString = root.end({ pretty: true });
  const fileName = 'sitemap_index_counts.xml';

  await uploadToS3(fileName, xmlString);
  const fileUrl = `https://www.jobtrees.com/api/sitemap/${fileName}`;
  console.log(`✅ Index count sitemap uploaded: ${fileUrl}`);
};

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

(async () => {
  try {
    const indicess = [
      'jobtrees_postings',
      'indeed_jobs_postings',
      'big_job_site_postings',
      'perengo_postings',
      'adzuna_postings',
    ];
    await generateIndexCountSitemap(indicess);
  } catch (error) {
    console.error('Error during the process:', error.message);
  } finally {
    rl.close();
  }
})();
