import { S3Client, PutObjectCommand, ListObjectsV2Command, DeleteObjectCommand } from '@aws-sdk/client-s3';
import aws4 from 'aws4';
import { create } from 'xmlbuilder';
import dotenv from 'dotenv';
import pLimit from 'p-limit';

dotenv.config({ path: '.env.local' });
const limit = pLimit(15);

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
  { city: "new york", state: "new york", stateAbbr: "ny" },
  { city: "los angeles", state: "california", stateAbbr: "ca" },
  { city: "chicago", state: "illinois", stateAbbr: "il" },
  { city: "houston", state: "texas", stateAbbr: "tx" },
  { city: "phoenix", state: "arizona", stateAbbr: "az" },
  { city: "philadelphia", state: "pennsylvania", stateAbbr: "pa" },
  { city: "san antonio", state: "texas", stateAbbr: "tx" },
  { city: "san diego", state: "california", stateAbbr: "ca" },
  { city: "dallas", state: "texas", stateAbbr: "tx" },
  { city: "jacksonville", state: "florida", stateAbbr: "fl" },
  { city: "austin", state: "texas", stateAbbr: "tx" },
  { city: "fort worth", state: "texas", stateAbbr: "tx" },
  { city: "san jose", state: "california", stateAbbr: "ca" },
  { city: "columbus", state: "ohio", stateAbbr: "oh" },
  { city: "charlotte", state: "north carolina", stateAbbr: "nc" },
  { city: "indianapolis", state: "indiana", stateAbbr: "in" },
  { city: "san francisco", state: "california", stateAbbr: "ca" },
  { city: "seattle", state: "washington", stateAbbr: "wa" },
  { city: "denver", state: "colorado", stateAbbr: "co" },
  { city: "oklahoma city", state: "oklahoma", stateAbbr: "ok" },
  { city: "nashville", state: "tennessee", stateAbbr: "tn" },
  { city: "washington", state: "district of columbia", stateAbbr: "dc" },
  { city: "el paso", state: "texas", stateAbbr: "tx" },
  { city: "las vegas", state: "nevada", stateAbbr: "nv" },
  { city: "boston", state: "massachusetts", stateAbbr: "ma" },
  { city: "detroit", state: "michigan", stateAbbr: "mi" },
  { city: "portland", state: "oregon", stateAbbr: "or" },
  { city: "louisville", state: "kentucky", stateAbbr: "ky" },
  { city: "memphis", state: "tennessee", stateAbbr: "tn" },
  { city: "baltimore", state: "maryland", stateAbbr: "md" },
  { city: "milwaukee", state: "wisconsin", stateAbbr: "wi" },
  { city: "albuquerque", state: "new mexico", stateAbbr: "nm" },
  { city: "tucson", state: "arizona", stateAbbr: "az" },
  { city: "fresno", state: "california", stateAbbr: "ca" },
  { city: "sacramento", state: "california", stateAbbr: "ca" },
  { city: "mesa", state: "arizona", stateAbbr: "az" },
  { city: "atlanta", state: "georgia", stateAbbr: "ga" },
  { city: "kansas city", state: "missouri", stateAbbr: "mo" },
  { city: "colorado springs", state: "colorado", stateAbbr: "co" },
  { city: "omaha", state: "nebraska", stateAbbr: "ne" },
  { city: "raleigh", state: "north carolina", stateAbbr: "nc" },
  { city: "miami", state: "florida", stateAbbr: "fl" },
  { city: "virginia beach", state: "virginia", stateAbbr: "va" },
  { city: "long beach", state: "california", stateAbbr: "ca" },
  { city: "oakland", state: "california", stateAbbr: "ca" },
  { city: "minneapolis", state: "minnesota", stateAbbr: "mn" },
  { city: "bakersfield", state: "california", stateAbbr: "ca" },
  { city: "tulsa", state: "oklahoma", stateAbbr: "ok" },
  { city: "tampa", state: "florida", stateAbbr: "fl" },
  { city: "arlington", state: "texas", stateAbbr: "tx" },
];

const formatRoleName = (role) => role.toLowerCase().replace(/-/g, "--").replace(/ /g, "-");
const formatCityName = (city) => city.toLowerCase().replace(/ /g, "-");
const generateUrl = (role, city, stateAbbr) => `https://www.jobtrees.com/browse-careers/${formatRoleName(role)}-jobs-in-${formatCityName(city)}-${stateAbbr.toLowerCase()}`;


// const fetchRoles = async () => {
//   return ["software engineer", "product manager", "marketing specialist"];
// };

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

  const searchRequests = indices.map(async (indexName) => {
    const searchRequestBody = {
      query: {
        bool: {
          must: [
            { terms: { "jobTreesTitle.keyword": [role.toLowerCase()] } },
            { term: { "city.keyword": city.toLowerCase() } },
            { term: { "state.keyword": state.toLowerCase().trim() } },
          ],
        },
      },
      size: 1,
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

    // console.log(`\n🔍 Querying index: "${indexName}" with request body:`, JSON.stringify(searchRequestBody, null, 2));

    try {
      const searchResponse = await fetch(`https://${searchRequest.host}${searchRequest.path}`, {
        method: searchRequest.method,
        headers: searchRequest.headers,
        body: searchRequest.body,
      });

      const searchResponseBody = await searchResponse.json();
      // console.log(`📌 Response from "${indexName}":`, JSON.stringify(searchResponseBody, null, 2));

      const jobCount = searchResponseBody.hits?.total?.value || 0;
      console.log(`✅ Found ${jobCount} jobs for "${role}" in "${city}, ${state}" from "${indexName}"`);

      return jobCount > 0;
    } catch (error) {
      console.error(`❌ Error checking jobs in index "${indexName}" for "${role}" in "${city}, ${state}":`, error);
      return false;
    }
  });

  const results = await Promise.all(searchRequests);
  const jobExists = results.some(result => result);

  if (jobExists) {
    console.log(`✅ Job exists for "${role}" in "${city}, ${state}" ✅`);
  } else {
    console.log(`🚫 No job found for "${role}" in "${city}, ${state}" 🚫`);
  }

  return jobExists;
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

const deleteOutdatedSitemaps = async () => {
  try {
    const existingFiles = await getExistingSitemapFiles();
    console.log('Existing files in S3:', existingFiles);
    const datePattern = /pSEO_(\d{4}-\d{2}-\d{2})\.xml/;
    const filesToDelete = [];

    // Filter files based on date
    const datedFiles = existingFiles.filter(file => datePattern.test(file));
    const filesWithDates = datedFiles.map(file => {
      const match = file.match(datePattern);
      return match ? { file, date: match[1] } : null;
    }).filter(Boolean);

    // Sort files by date (desc)
    filesWithDates.sort((a, b) => new Date(b.date) - new Date(a.date));

    // Keep the latest 2 days and prepare files to delete
    const filesToKeep = filesWithDates.slice(0, 2);
    const filesToDeleteSet = new Set(filesWithDates.slice(2).map(f => f.file));

    // Prepare the list of files to delete
    for (const file of existingFiles) {
      if (filesToDeleteSet.has(file)) {
        filesToDelete.push(file);
      }
    }

    // Check and delete outdated files
    for (const file of filesToDelete) {
      console.log(`Deleting ${file} from S3...`);
      await s3Client.send(new DeleteObjectCommand({ Bucket: S3_BUCKET, Key: file }));
      console.log(`File deleted: ${file}`);
    }

  } catch (error) {
    console.error('Error deleting outdated sitemaps from S3:', error);
  }
};

const checkJobExistsThrottled = async (role, city, state) => {
  return limit(() => checkJobExists(role, city, state));
};


const generateSitemapXml = async () => {
  const MAX_URLS_PER_SITEMAP = 5000;

  console.log('Generating URLs...');
  const locations = getStaticLocations();
  const roles = await fetchRoles();
  const urls = [];
  const jobCheckPromises = roles.flatMap(role =>
    locations.map(({ city, state, stateAbbr }) =>
      checkJobExistsThrottled(role, city, state).then((exists) => {
        if (exists) {
          urls.push(generateUrl(role, city, stateAbbr));
        }
      })
    )
  );

  await Promise.all(jobCheckPromises);
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
  const index = create('urlset', { version: '1.0', encoding: 'UTF-8' })
    .att('xmlns', 'http://www.sitemaps.org/schemas/sitemap/0.9');

  sitemapFiles.forEach(file => {
    index.ele('url')
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

(async () => {
  await deleteOutdatedSitemaps();
  await generateSitemapXml();
})();