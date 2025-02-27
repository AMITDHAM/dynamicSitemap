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
  { city: "New York", state: "New York", stateAbbr: "NY" },
  { city: "Los Angeles", state: "California", stateAbbr: "CA" },
  { city: "Chicago", state: "Illinois", stateAbbr: "IL" },
  { city: "Houston", state: "Texas", stateAbbr: "TX" },
  // Add the rest of the locations here...
];

const formatRoleName = (role) => role.toLowerCase().replace(/-/g, "--").replace(/ /g, "-");
const formatCityName = (city) => city.toLowerCase().replace(/ /g, "-");

const generateUrl = (role, city, stateAbbr) => {
  return `https://www.jobtrees.com/browse-careers/${formatRoleName(role)}-jobs-in-${formatCityName(city)}-${stateAbbr.toLowerCase()}`;
};

const fetchRoles = async () => {
  return ["software engineer", "data analyst", "product manager", "marketing specialist"];
};


// const fetchRoles = async () => {
//     try {
//       const response = await fetch('https://api.qa.jobtrees.com/roles/roleList');
//       return await response.json();
//     } catch (error) {
//       console.error('Error fetching roles:', error);
//       return [];
//     }
//   };

const checkJobExists = async (role, city, state) => {
  console.log(`Checking jobs for role: ${role}, city: ${city}, state: ${state}`);
  const indices = [
    'adzuna_postings', 'big_job_site_postings', 'indeed_jobs_postings', 'jobtrees_postings', 'greenhouse_postings'
  ];
  for (const indexName of indices) {
    const searchRequestBody = {
      query: {
        bool: {
          must: [
            { terms: { "jobTreesTitles.keyword": [role] } },
            { term: { "city.keyword": city.toLowerCase() } },
            { term: { "state.keyword": state.toLowerCase().trim() } }
          ]
        }
      },
      size: 1
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

    try {
      const searchResponse = await fetch(`https://${searchRequest.host}${searchRequest.path}`, {
        method: searchRequest.method,
        headers: searchRequest.headers,
        body: searchRequest.body,
      });
      const searchResponseBody = await searchResponse.json();
      console.log(`Elasticsearch response for ${role} in ${city}, ${state}:`, JSON.stringify(searchResponseBody, null, 2));
      if (searchResponseBody.hits && searchResponseBody.hits.total && searchResponseBody.hits.total.value > 0) return true;
    } catch (error) {
      console.error(`Error checking jobs for ${role} in ${city}, ${state}:`, error);
    }
  }
  return false;
};

const getExistingSitemapFiles = async () => {
  const params = {
    Bucket: S3_BUCKET,
    Prefix: S3_PUBLIC_PATH,
  };
  try {
    const response = await s3Client.send(new ListObjectsV2Command(params));
    return response.Contents ? response.Contents.map(item => item.Key) : [];
  } catch (error) {
    console.error('Error fetching existing sitemaps:', error);
    return [];
  }
};

const deleteSitemapFromS3 = async (fileName) => {
  const params = {
    Bucket: S3_BUCKET,
    Key: fileName,
  };
  try {
    const command = new DeleteObjectCommand(params);
    await s3Client.send(command);
    console.log(`File deleted: ${fileName}`);
  } catch (error) {
    console.error(`Error deleting ${fileName} from S3:`, error);
  }
};

const generateSitemapXml = async () => {
  const existingFiles = await getExistingSitemapFiles();
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

  const root = create('urlset').att('xmlns', 'http://www.sitemaps.org/schemas/sitemap/0.9');
  urls.forEach((url) => {
    const urlElement = root.ele('url');
    urlElement.ele('loc', url);
    urlElement.ele('lastmod', new Date().toISOString());
    urlElement.ele('changefreq', 'daily');
    urlElement.ele('priority', '1.0');
  });

  const fileName = 'sitemap_browse_careers.xml';
  if (!urls.length && existingFiles.includes(`${S3_PUBLIC_PATH}${fileName}`)) {
    await deleteSitemapFromS3(`${S3_PUBLIC_PATH}${fileName}`);
    console.log(`Deleted sitemap ${fileName} as no jobs were found.`);
    return;
  }
  await uploadToS3(fileName, root.end({ pretty: true }));
  console.log('Sitemap generated and uploaded successfully.');
};

const uploadToS3 = async (fileName, fileContent) => {
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
    console.log(`File uploaded: ${fileName}`);
  } catch (error) {
    console.error(`Error uploading ${fileName} to S3:`, error);
  }
};

(async () => {
  await generateSitemapXml();
})();
