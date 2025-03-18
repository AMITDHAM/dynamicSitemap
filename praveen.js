import { S3Client, PutObjectCommand, ListObjectsV2Command, DeleteObjectCommand } from '@aws-sdk/client-s3';
import aws4 from 'aws4';
import { create } from 'xmlbuilder';
import dotenv from 'dotenv';
import pLimit from 'p-limit';

dotenv.config({ path: '.env.local' });
const limit = pLimit(20);

const region = process.env.region;
const credentials = {
  accessKeyId: process.env.accessKeyId,
  secretAccessKey: process.env.secretAccessKey,
};
const S3_BUCKET = process.env.S3_BUCKET;
const S3_PUBLIC_PATH = process.env.S3_PUBLIC_PATH2;
const S3_PUBLIC_PATH_CITY = process.env.S3_PUBLIC_PATH3;
const S3_PUBLIC_PATH_ROLE = process.env.S3_PUBLIC_PATH4;
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
    const response = await fetch('https://api.jobtrees.com/roles/roleList');
    const data = await response.json();
    return data;
  } catch (error) {
    console.error('Error fetching roles:', error);
    return [];
  }
};

const checkJobExists = async (type, role, city, state) => {
  const indices = [
    'jobtrees_postings',
    'indeed_jobs_postings',
    'big_job_site_postings',
    'adzuna_postings',
  ];

  const searchRequests = indices.map(async (indexName) => {
    let searchRequestBody;
    if (type === 'all') {
      searchRequestBody = {
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
    }
    else if (type === 'role') {
      searchRequestBody = {
        query: {
          bool: {
            must: [
              { terms: { "jobTreesTitle.keyword": [role.toLowerCase()] } },
            ],
          },
        },
        size: 1,
      };
    }
    else if (type === 'city') {
      searchRequestBody = {
        query: {
          bool: {
            must: [
              { term: { "city.keyword": city.toLowerCase() } },
              { term: { "state.keyword": state.toLowerCase().trim() } },
            ],
          },
        },
        size: 1,
      };
    }

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
    try {
      const searchResponse = await fetch(`https://${searchRequest.host}${searchRequest.path}`, {
        method: searchRequest.method,
        headers: searchRequest.headers,
        body: searchRequest.body,
      });

      const searchResponseBody = await searchResponse.json();
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
  return jobExists;
};


const getExistingSitemapFiles = async () => {
  try {
    console.log('Fetching existing sitemaps...');

    const paths = [S3_PUBLIC_PATH, S3_PUBLIC_PATH_CITY, S3_PUBLIC_PATH_ROLE];
    let allFiles = [];

    for (const path of paths) {
      const response = await s3Client.send(new ListObjectsV2Command({ Bucket: S3_BUCKET, Prefix: path }));
      if (response.Contents) {
        const existingFiles = response.Contents.map(file => ({
          key: file.Key,
          lastModified: file.LastModified,
        }));
        
        // If you still want to combine the `existingFiles` with `allFiles`, you can use concat:
        allFiles = allFiles.concat(existingFiles);
      }
    }
    return allFiles;
  } catch (error) {
    console.error('Error fetching existing sitemaps:', error);
    return [];
  }
};


const deleteOutdatedSitemaps = async () => {
  try {
    const existingFiles = await getExistingSitemapFiles();
    console.log('Existing files in S3:', existingFiles);

    const now = new Date();
    const twentyFourHoursAgo = new Date(now - 24 * 60 * 60 * 1000); // 24 hours ago

    // Filter files based on lastModified date
    const filesToDelete = existingFiles.filter(file => {
      const lastModifiedDate = new Date(file.lastModified);
      return lastModifiedDate < twentyFourHoursAgo;
    }).map(file => file.key);

    if (filesToDelete.length === 0) {
      console.log('No outdated files found to delete.');
      return;
    }
    // Check and delete outdated files
    for (const fileKey of filesToDelete) {
      console.log(`Deleting ${fileKey} from S3...`);
      await s3Client.send(new DeleteObjectCommand({ Bucket: S3_BUCKET, Key: fileKey }));
      console.log(`File deleted: ${fileKey}`);
    }

  } catch (error) {
    console.error('Error deleting outdated sitemaps from S3:', error);
  }
  
};


const checkJobExistsThrottled = async (type, role, city, state) => {
  return limit(() => checkJobExists(type, role, city, state));
};


const generateSitemapXml = async (type) => {
  const MAX_URLS_PER_SITEMAP = 5000;

  console.log('Generating URLs...');
  const locations = getStaticLocations();
  const roles = await fetchRoles();
  const urls = [];
  
  console.log(`Generating URLs for sitemap type: ${type}`);

  if (type === 'role') {
    await Promise.all(
      roles.map(role =>
        checkJobExistsThrottled(type, role, null, null).then((exists) => {
          if (exists) urls.push(`https://www.jobtrees.com/browse-careers/${formatRoleName(role)}`);
        })
      )
    );
  } else if (type === 'city') {
    await Promise.all(
      locations.map(({ city, state, stateAbbr }) =>
        checkJobExistsThrottled(type, null, city, state).then((exists) => {
          if (exists) urls.push(`https://www.jobtrees.com/browse-careers/${formatCityName(city)}-${stateAbbr.toLowerCase()}`);
        })
      )
    );
  } else {
    await Promise.all(
      roles.flatMap(role =>
        locations.map(({ city, state, stateAbbr }) =>
          checkJobExistsThrottled(type, role, city, state).then((exists) => {
            if (exists) urls.push(generateUrl(role, city, stateAbbr));
          })
        )
      )
    );
  }

  console.log(`Generated ${urls.length} URLs for type: ${type}`);

  let sitemapIndex = 1;
  let currentUrls = [];
  let sitemapFiles = [];

  // Determine file name prefix based on type
  let filePrefix, indexFile;
  if (type === 'role') {
    filePrefix = 'sitemap_browse_role';
    indexFile = 'sitemap_index_browse_role.xml';
  } else if (type === 'city') {
    filePrefix = 'sitemap_browse_city';
    indexFile = 'sitemap_index_browse_city.xml';
  } else {
    filePrefix = 'pSEO_page';
    indexFile = 'sitemap_index_pSEO.xml';
  }

  // Generate and upload individual sitemap files
  for (const url of urls) {
    if (currentUrls.length >= MAX_URLS_PER_SITEMAP) {
      const sitemapFileName = `${filePrefix}_${sitemapIndex}.xml`;
      await uploadToS3(type, sitemapFileName, generateSitemapXmlContent(currentUrls));
      sitemapFiles.push(sitemapFileName);
      currentUrls = [];
      sitemapIndex++;
    }
    currentUrls.push(url);
  }

  if (currentUrls.length > 0) {
    const sitemapFileName = `${filePrefix}_${sitemapIndex}.xml`;
    await uploadToS3(type, sitemapFileName, generateSitemapXmlContent(currentUrls));
    sitemapFiles.push(sitemapFileName);
  }

  // Upload the sitemap index
  await uploadToS3(type, indexFile, generateSitemapIndex(sitemapFiles, type));

  console.log(`Sitemap ${type} generated and uploaded successfully.`);
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

const generateSitemapIndex = (sitemapFiles, type) => {
  const index = create('urlset', { version: '1.0', encoding: 'UTF-8' })
    .att('xmlns', 'http://www.sitemaps.org/schemas/sitemap/0.9');
    let fullFilePaths;
    if (type === 'role') {
      fullFilePaths = `${S3_PUBLIC_PATH_ROLE}`;
    } else if (type === 'city') {
      fullFilePaths = `${S3_PUBLIC_PATH_CITY}`;
    } else {
      fullFilePaths = `${S3_PUBLIC_PATH}`;
    }
  sitemapFiles.forEach(file => {
    index.ele('url')
      .ele('loc', `https://www.jobtrees.com/api/${fullFilePaths}${file}`).up()
      .ele('lastmod', new Date().toISOString()).up()
      .ele('changefreq', 'daily').up()
      .ele('priority', '1.0').up();
  });

  return index.end({ pretty: true });
};

const uploadToS3 = async (type, fileName, fileContent) => {
  try {
    let fullFilePath;
    if (type === 'role') {
      fullFilePath = `${S3_PUBLIC_PATH_ROLE}${fileName}`;
    } else if (type === 'city') {
      fullFilePath = `${S3_PUBLIC_PATH_CITY}${fileName}`;
    } else {
      fullFilePath = `${S3_PUBLIC_PATH}${fileName}`;
    }
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
  // await getExistingSitemapFiles()
  await deleteOutdatedSitemaps();
  await generateSitemapXml('role');
  await generateSitemapXml('city');
  await generateSitemapXml('all');
})();