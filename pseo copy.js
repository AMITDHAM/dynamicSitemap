import {
  S3Client,
  PutObjectCommand,
} from '@aws-sdk/client-s3';
import { create } from 'xmlbuilder';
import dotenv from 'dotenv';
import pLimit from 'p-limit';
import aws4 from 'aws4';
import fs from 'fs';

dotenv.config({ path: '.env.local' });

const limit = pLimit(30);
const region = process.env.region;
const credentials = {
  accessKeyId: process.env.accessKeyId,
  secretAccessKey: process.env.secretAccessKey,
};
const S3_BUCKET = process.env.S3_BUCKET;
const OPEN_SEARCH_URL = process.env.OPEN_SEARCH_URL;
const PATHS = {
  all: process.env.S3_PUBLIC_PATH2,
  city: process.env.S3_PUBLIC_PATH3,
  role: process.env.S3_PUBLIC_PATH4,
  company: process.env.S3_PUBLIC_PATH6,
  remote: process.env.S3_PUBLIC_PATH7 || 'sitemap_remote/',
};

const s3Client = new S3Client({ region, credentials });


const getLocations = () => {
  const data = JSON.parse(fs.readFileSync('./cities.json', 'utf-8'));
  const flattened = [];
  for (const [state, cities] of Object.entries(data)) {
    cities.forEach(city => {
      flattened.push({
        city: city.cityName.toLowerCase(),
        state: state.toLowerCase(),
        stateAbbr: city.abbreviation.toLowerCase(),
        zipCodes: city.zipCodes || []
      });
    });
  }
  return flattened;
};

/**
 * Load roles.json
 */
const fetchRoles = async () => {
  const roles = JSON.parse(fs.readFileSync('./roles.json', 'utf-8'));
  return roles; // array of strings
};

const formatRole = (r) => r.toLowerCase().replace(/-/g, '--').replace(/ /g, '-');
const formatCity = (c) => c.toLowerCase().replace(/ /g, '-');
const buildUrl = (r, c, s) =>
  `https://www.jobtrees.com/browse-careers/${formatRole(r)}-jobs-in-${formatCity(c)}-${s}`;

const createXmlContent = (urls) =>
  create('urlset', { version: '1.0', encoding: 'UTF-8' })
    .att('xmlns', 'http://www.sitemaps.org/schemas/sitemap/0.9')
    .ele(
      urls.map(url => ({
        url: {
          loc: url,
          lastmod: new Date().toISOString(),
          changefreq: 'daily',
          priority: '1.0'
        }
      }))
    )
    .end({ pretty: true });

const createIndex = (files, pathType) => {
  const index = create('urlset', { version: '1.0', encoding: 'UTF-8' })
    .att('xmlns', 'http://www.sitemaps.org/schemas/sitemap/0.9');

  let fullFilePaths;
  if (pathType === 'role') fullFilePaths = PATHS.role;
  else if (pathType === 'city') fullFilePaths = PATHS.city;
  else if (pathType === 'company') fullFilePaths = PATHS.company;
  else if (pathType === 'remote') fullFilePaths = PATHS.remote;
  else fullFilePaths = `sitemap_pSEO/`;

  files.forEach(file => {
    index.ele('url')
      .ele('loc', `https://www.jobtrees.com/api/${fullFilePaths}${file}`).up()
      .ele('lastmod', new Date().toISOString()).up()
      .ele('changefreq', 'daily').up()
      .ele('priority', '1.0').up();
  });

  return index.end({ pretty: true });
};

const uploadToS3 = async (type, fileName, content) => {
  try {
    const path = `${PATHS[type]}${fileName}`;
    console.log(`Uploading: ${path}`);
    await s3Client.send(new PutObjectCommand({
      Bucket: S3_BUCKET,
      Key: path,
      Body: content,
      ContentType: 'application/xml',
      ACL: 'public-read',
    }));
  } catch (e) {
    console.error(`Upload failed for ${fileName}:`, e);
  }
};

const checkJobExists = async (type, role, city, state) => {
  const indices = [
    'jobtrees_postings',
    'indeed_jobs_postings',
    'big_job_site_postings',
    'perengo_postings',
    'adzuna_postings',
  ];

  const searchRequests = indices.map(async (indexName) => {
    let searchRequestBody;

    // Base filter for required fields
    const mustFilters = [
      { exists: { field: "postcode" } },
      { exists: { field: "company" } },
      { exists: { field: "title" } },
      { exists: { field: "state" } },
      { exists: { field: "city" } },
      { term: { "status.keyword": "Active" } }
    ];

    if (type === 'all') {
      mustFilters.push({ match: { "jobTreesTitle.keyword": role?.toLowerCase() } });
      mustFilters.push({ term: { "city.keyword": city } });
      mustFilters.push({ term: { "state.keyword": state.trim() } });
    } else if (type === 'role') {
      mustFilters.push({ match: { "jobTreesTitle.keyword": role?.toLowerCase() } });
    } else if (type === 'city') {
      mustFilters.push({ term: { "city.keyword": city } });
      mustFilters.push({ term: { "state.keyword": state.trim() } });
    }

    searchRequestBody = { query: { bool: { must: mustFilters } }, size: 1 };

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
      return jobCount > 0;
    } catch (error) {
      console.error(`❌ Error checking jobs in "${indexName}"`, error);
      return false;
    }
  });

  const results = await Promise.all(searchRequests);
  return results.some(result => result);
};

const checkJobExistsThrottled = (type, role, city, state) =>
  limit(() => checkJobExists(type, role, city, state));

const generateSitemap = async (type) => {
  const MAX = 5000;
  const locations = getLocations();
  const roles = await fetchRoles();
  let urls = [];

  if (type === 'role') {
    for (const role of roles) {
      const hasJobs = await checkJobExistsThrottled(type, role, null, null);
      if (hasJobs) urls.push(`https://www.jobtrees.com/browse-careers/${formatRole(role)}`);
    }
  } else if (type === 'city') {
    for (const location of locations) {
      const hasJobs = await checkJobExistsThrottled(type, null, location.city, location.state);
      if (hasJobs) urls.push(`https://www.jobtrees.com/browse-careers/${formatCity(location.city)}-${location.stateAbbr}`);
    }
  } else if (type === 'company') {
    urls = locations.map(({ city, stateAbbr }) =>
      `https://www.jobtrees.com/top-companies/${formatCity(city)}-${stateAbbr}`
    );
    urls.push('https://www.jobtrees.com/top-companies/remote-us');
  } else {
    for (const role of roles) {
      for (const location of locations) {
        const hasJobs = await checkJobExistsThrottled('all', role, location.city, location.state);
        if (hasJobs) urls.push(buildUrl(role, location.city, location.stateAbbr));
      }
    }
  }

  const prefix = type === 'role'
    ? 'sitemap_browse_role'
    : type === 'city'
      ? 'sitemap_browse_city'
      : type === 'company'
        ? 'sitemap_browse_company'
        : 'pSEO_page';
  const indexFile = type === 'role'
    ? 'sitemap_index_browse_role.xml'
    : type === 'city'
      ? 'sitemap_index_browse_city.xml'
      : type === 'company'
        ? 'sitemap_index_browse_company.xml'
        : 'sitemap_index_pSEO.xml';

  const chunks = [];
  for (let i = 0; i < urls.length; i += MAX) chunks.push(urls.slice(i, i + MAX));

  const sitemapFiles = await Promise.all(chunks.map((chunk, i) =>
    limit(async () => {
      const file = `${prefix}_${i + 1}.xml`;
      const xml = createXmlContent(chunk);
      await uploadToS3(type, file, xml);
      return file;
    })
  ));

  const indexXml = createIndex(sitemapFiles, type);
  await uploadToS3(type, indexFile, indexXml);
  console.log(`${type} sitemap complete. ${urls.length} URLs included.`);
};

(async () => {
  await Promise.all([
    generateSitemap('role'),
    generateSitemap('city'),
    generateSitemap('company'),
    generateSitemap('all'),
  ]);
})();
