import { S3Client, PutObjectCommand, ListObjectsV2Command, DeleteObjectCommand } from '@aws-sdk/client-s3';
import aws4 from 'aws4';
import { create } from 'xmlbuilder';
import dotenv from 'dotenv';
import readline from 'readline';

dotenv.config({ path: '.env.local' });
const MAX_RETRIES = 3;
const RETRY_DELAY = 1000; // Initial delay for retry (1 second)
const MAX_URLS_PER_SITEMAP = 5000;

const region = process.env.region;
const credentials = {
  accessKeyId: process.env.accessKeyId,
  secretAccessKey: process.env.secretAccessKey,
};
const S3_BUCKET = process.env.S3_BUCKET;
const S3_PUBLIC_PATH = process.env.S3_PUBLIC_PATH2;
const OPEN_SEARCH_URL = process.env.OPEN_SEARCH_URL;

const s3Client = new S3Client({ region, credentials });

const retryWithBackoff = async (fn, retries = MAX_RETRIES) => {
  for (let i = 0; i <= retries; i++) {
    try {
      return await fn();
    } catch (error) {
      if (i === retries) throw error;
      console.warn(`Retrying (${i + 1}/${retries}) due to error:`, error.message);
      await new Promise(res => setTimeout(res, RETRY_DELAY * (2 ** i))); // Exponential backoff
    }
  }
};

const fetchRoles = async () => retryWithBackoff(async () => {
  console.log('Fetching roles...');
  const response = await fetch('https://api.jobtrees.com/roles/roleList');
  return response.json();
});

const checkJobExists = async (role, city, state) => retryWithBackoff(async () => {
  console.log(`Checking job for ${role} in ${city}, ${state}...`);
  const indices = ['adzuna_postings', 'big_job_site_postings', 'indeed_jobs_postings', 'jobtrees_postings', 'greenhouse_postings'];

  const searchRequests = indices.map(index => {
    const searchRequestBody = {
      query: { bool: { must: [
        { terms: { "jobTreesTitles.keyword": [role] } },
        { term: { "city.keyword": city.toLowerCase() } },
        { term: { "state.keyword": state.toLowerCase().trim() } }
      ] } },
      size: 1,
    };
    
    const searchRequest = {
      host: OPEN_SEARCH_URL,
      path: `/${index}/_search`,
      service: 'es',
      region,
      method: 'POST',
      body: JSON.stringify(searchRequestBody),
      headers: { 'Content-Type': 'application/json' },
    };

    aws4.sign(searchRequest, credentials);
    return fetch(`https://${searchRequest.host}${searchRequest.path}`, {
      method: searchRequest.method,
      headers: searchRequest.headers,
      body: searchRequest.body,
    }).then(res => res.json()).then(data => data.hits?.total?.value > 0);
  });

  const results = await Promise.allSettled(searchRequests);
  return results.some(result => result.status === 'fulfilled' && result.value);
});

const getExistingSitemapFiles = async () => retryWithBackoff(async () => {
  const response = await s3Client.send(new ListObjectsV2Command({ Bucket: S3_BUCKET, Prefix: S3_PUBLIC_PATH }));
  return response.Contents ? response.Contents.map(item => item.Key) : [];
});

const deleteOutdatedSitemaps = async () => {
  const existingFiles = await getExistingSitemapFiles();
  const datePattern = /pSEO_(\d{4}-\d{2}-\d{2})\.xml/;
  
  const datedFiles = existingFiles.filter(file => datePattern.test(file))
    .map(file => ({ file, date: file.match(datePattern)[1] }))
    .sort((a, b) => new Date(b.date) - new Date(a.date));
  
  const filesToDelete = datedFiles.slice(2).map(f => f.file);
  await Promise.allSettled(filesToDelete.map(file => 
    retryWithBackoff(() => s3Client.send(new DeleteObjectCommand({ Bucket: S3_BUCKET, Key: file })))
  ));
};

const generateSitemapXml = async () => {
  console.log('Generating sitemap...');
  const roles = await fetchRoles();
  const locations = getStaticLocations();
  const urls = [];

  await Promise.allSettled(roles.flatMap(role =>
    locations.map(({ city, state, stateAbbr }) =>
      checkJobExists(role, city, state).then(exists => {
        if (exists) urls.push(generateUrl(role, city, stateAbbr));
      })
    )
  ));

  console.log(`Generated ${urls.length} URLs.`);

  let sitemapFiles = [];
  for (let i = 0; i < urls.length; i += MAX_URLS_PER_SITEMAP) {
    const sitemapFileName = `pSEO_${new Date().toISOString().split('T')[0]}_${Math.floor(i / MAX_URLS_PER_SITEMAP) + 1}.xml`;
    await uploadToS3(sitemapFileName, generateSitemapXmlContent(urls.slice(i, i + MAX_URLS_PER_SITEMAP)));
    sitemapFiles.push(sitemapFileName);
  }
  await uploadToS3('sitemap_index_pSEO.xml', generateSitemapIndex(sitemapFiles));
};

const uploadToS3 = async (fileName, fileContent) => retryWithBackoff(async () => {
  await s3Client.send(new PutObjectCommand({
    Bucket: S3_BUCKET,
    Key: `${S3_PUBLIC_PATH}${fileName}`,
    Body: fileContent,
    ContentType: 'application/xml',
    ACL: 'public-read',
  }));
});

(async () => {
  await deleteOutdatedSitemaps();
  await generateSitemapXml();
})();
