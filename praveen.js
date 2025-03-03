const { S3Client, PutObjectCommand, ListObjectsV2Command, DeleteObjectCommand } = require('@aws-sdk/client-s3');
const aws4 = require('aws4');
const { create } = require('xmlbuilder');
require('dotenv').config({ path: '.env.local' });

const region = process.env.region;
const credentials = {
  accessKeyId: process.env.accessKeyId,
  secretAccessKey: process.env.secretAccessKey,
};
const S3_BUCKET = process.env.S3_BUCKET;
const S3_PUBLIC_PATH = process.env.S3_PUBLIC_PATH2;
const OPEN_SEARCH_URL = process.env.OPEN_SEARCH_URL;

const s3Client = new S3Client({ region, credentials });

const getStaticLocations = () => [
  { city: "Nashville", state: "Tennessee", stateAbbr: "TN" },
  { city: "Washington", state: "District of Columbia", stateAbbr: "DC" },
  { city: "El Paso", state: "Texas", stateAbbr: "TX" },
  { city: "Las Vegas", state: "Nevada", stateAbbr: "NV" },
  { city: "Boston", state: "Massachusetts", stateAbbr: "MA" },
  { city: "Detroit", state: "Michigan", stateAbbr: "MI" },
  { city: "Portland", state: "Oregon", stateAbbr: "OR" },
  { city: "Louisville", state: "Kentucky", stateAbbr: "KY" },
  { city: "Memphis", state: "Tennessee", stateAbbr: "TN" },
  { city: "Baltimore", state: "Maryland", stateAbbr: "MD" },
];
const formatRoleName = (role) => role.toLowerCase().replace(/-/g, "--").replace(/ /g, "-");
const formatCityName = (city) => city.toLowerCase().replace(/ /g, "-");
const generateUrl = (role, city, stateAbbr) => `https://www.jobtrees.com/browse-careers/${formatRoleName(role)}-jobs-in-${formatCityName(city)}-${stateAbbr.toLowerCase()}`;

const fetchRoles = async () => {
  try {
    console.log('Fetching roles...');
    const response = await fetch('https://api.jobtrees.com/roles/roleList');
    const data = await response.json();
    console.log('Roles fetched successfully:', data);
    return data;
  } catch (error) {
    console.error('Error fetching roles:', error);
    return [];
  }
};

const checkJobExists = async (role, city, state) => {
  console.log(`Checking if job exists for ${role} in ${city}, ${state}...`);
  const indices = ['adzuna_postings', 'big_job_site_postings', 'indeed_jobs_postings', 'jobtrees_postings', 'greenhouse_postings'];
  for (const indexName of indices) {
    const searchRequestBody = { query: { bool: { must: [{ terms: { "jobTreesTitles.keyword": [role] } }, { term: { "city.keyword": city.toLowerCase() } }, { term: { "state.keyword": state.toLowerCase().trim() } }] } }, size: 1 };
    const searchRequest = { host: OPEN_SEARCH_URL, path: `/${indexName}/_search`, service: 'es', region, method: 'POST', body: JSON.stringify(searchRequestBody), headers: { 'Content-Type': 'application/json' } };
    aws4.sign(searchRequest, credentials);
    try {
      const searchResponse = await fetch(`https://${searchRequest.host}${searchRequest.path}`, { method: searchRequest.method, headers: searchRequest.headers, body: searchRequest.body });
      const searchResponseBody = await searchResponse.json();
      if (searchResponseBody.hits?.total?.value > 0) {
        console.log(`Job found for ${role} in ${city}, ${state}`);
        return true;
      }
    } catch (error) {
      console.error(`Error checking jobs for ${role} in ${city}, ${state}:`, error);
    }
  }
  console.log(`No job found for ${role} in ${city}, ${state}`);
  return false;
};

const getExistingSitemapFiles = async () => {
  try {
    console.log('Fetching existing sitemaps...');
    const response = await s3Client.send(new ListObjectsV2Command({ Bucket: S3_BUCKET, Prefix: S3_PUBLIC_PATH }));
    return response.Contents ? response.Contents.map(item => item.Key) : [];
  } catch (error) {
    console.error('Error fetching existing sitemaps:', error);
    return [];
  }
};

const deleteSitemapFromS3 = async () => {
  try {
    const existingFiles = await getExistingSitemapFiles();
    console.log('Existing files in S3:', existingFiles);
    const filesToDelete = existingFiles.filter(file =>
      file.startsWith('seo/pSEO_') || file.startsWith('seo/sitemap_index')
    );

    console.log('Files to delete:', filesToDelete);

    for (const file of filesToDelete) {
      console.log(`Deleting ${file} from S3...`);
      await s3Client.send(new DeleteObjectCommand({ Bucket: S3_BUCKET, Key: file }));
      console.log(`File deleted: ${file}`);
    }
  } catch (error) {
    console.error('Error deleting files from S3:', error);
  }
};

const generateSitemapXml = async () => {
  const MAX_URLS_PER_SITEMAP = 5000;
  console.log('Deleting existing sitemaps before generating a new one...');
  await deleteSitemapFromS3();

  console.log('Generating URLs...');
  const locations = getStaticLocations();
  const roles = await fetchRoles();
  const urls = [];
  for (const role of roles) {
    for (const { city, state, stateAbbr } of locations) {
      if (await checkJobExists(role, city, state)) {
        urls.push(generateUrl(role, city, stateAbbr));
      }
    }
  }

  console.log(`Generated ${urls.length} URLs for the sitemap`);

  let sitemapIndex = 1;
  let currentUrls = [];
  let sitemapFiles = [];
  for (const url of urls) {
    if (currentUrls.length >= MAX_URLS_PER_SITEMAP) {
      const sitemapFileName = `pSEO_page_${sitemapIndex}.xml`;
      await uploadToS3(sitemapFileName, generateSitemapXmlContent(currentUrls));
      sitemapFiles.push(sitemapFileName);
      currentUrls = [];
      sitemapIndex++;
    }
    currentUrls.push(url);
  }
  if (currentUrls.length > 0) {
    const sitemapFileName = `pSEO_page_${sitemapIndex}.xml`;
    await uploadToS3(sitemapFileName, generateSitemapXmlContent(currentUrls));
    sitemapFiles.push(sitemapFileName);
  }
  await uploadToS3('sitemap_index_pSEO.xml', generateSitemapIndex(sitemapFiles));
  console.log('Sitemap generated and uploaded successfully.');
};

const generateSitemapXmlContent = (urls) => {
  const urlset = create('urlset', { version: '1.0', encoding: 'UTF-8' })
    .att('xmlns', 'http://www.sitemaps.org/schemas/sitemap/0.9');

  urls.forEach(url => {
    urlset.ele('url')
      .ele('loc', url).up()
      .ele('lastmod', new Date().toISOString()).up()
      .ele('changefreq', 'daily').up()
      .ele('priority', '1.0').up();
  });

  return urlset.end({ pretty: true });
};

const generateSitemapIndex = (sitemapFiles) => {
  const index = create('sitemapindex', { version: '1.0', encoding: 'UTF-8' })
    .att('xmlns', 'http://www.sitemaps.org/schemas/sitemap/0.9');

  sitemapFiles.forEach(file => {
    index.ele('sitemap')
      .ele('loc', `https://www.jobtrees.com/api/sitemap_pSEO/${file}`).up()
      .ele('lastmod', new Date().toISOString()).up()
      .ele('changefreq', 'daily').up()
      .ele('priority', '1.0').up();
  });

  return index.end({ pretty: true });
};

const uploadToS3 = async (fileName, fileContent) => {
  try {
    const fullFilePath = `${S3_PUBLIC_PATH}${fileName}`;
    console.log(`Uploading ${fullFilePath} to S3...`);
    await s3Client.send(new PutObjectCommand({
      Bucket: S3_BUCKET,
      Key: fullFilePath,
      Body: fileContent,
      ContentType: 'application/xml',
      ACL: 'public-read'
    }));
    console.log(`File uploaded: ${fullFilePath}`);
  } catch (error) {
    console.error(`Error uploading ${fileName} to S3:`, error);
  }
};

(async () => { await generateSitemapXml(); })();
