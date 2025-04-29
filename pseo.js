import {
  S3Client,
  PutObjectCommand,
} from '@aws-sdk/client-s3';
import { create } from 'xmlbuilder';
import dotenv from 'dotenv';
import pLimit from 'p-limit';

dotenv.config({ path: '.env.local' });

const limit = pLimit(30); // Increased concurrency
const region = process.env.region;
const credentials = {
  accessKeyId: process.env.accessKeyId,
  secretAccessKey: process.env.secretAccessKey,
};
const S3_BUCKET = process.env.S3_BUCKET;
const PATHS = {
  all: process.env.S3_PUBLIC_PATH2,
  city: process.env.S3_PUBLIC_PATH3,
  role: process.env.S3_PUBLIC_PATH4,
  company: process.env.S3_PUBLIC_PATH6,
};

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

const formatRole = (r) => r.toLowerCase().replace(/-/g, '--').replace(/ /g, '-');
const formatCity = (c) => c.toLowerCase().replace(/ /g, '-');
const buildUrl = (r, c, s) => `https://www.jobtrees.com/browse-careers/${formatRole(r)}-jobs-in-${formatCity(c)}-${s}`;

const fetchRoles = async () => {
  try {
    const res = await fetch('https://api.jobtrees.com/roles/roleList');
    return await res.json();
  } catch (err) {
    console.error('Fetch roles error:', err);
    return [];
  }
};

const createXmlContent = (urls) =>
  create('urlset', { version: '1.0', encoding: 'UTF-8' })
    .att('xmlns', 'http://www.sitemaps.org/schemas/sitemap/0.9')
    .ele(urls.map(url => ({ url: { loc: url, lastmod: new Date().toISOString(), changefreq: 'daily', priority: '1.0' } })))
    .end({ pretty: true });

    const createIndex = (files, pathType) => {
  const index = create('urlset', { version: '1.0', encoding: 'UTF-8' })
    .att('xmlns', 'http://www.sitemaps.org/schemas/sitemap/0.9');
  let fullFilePaths;
  if (pathType === 'role') {
    fullFilePaths = PATHS[pathType];
  } else if (pathType === 'city') {
    fullFilePaths = PATHS[pathType];
  } else if (pathType === 'company') {
    fullFilePaths = PATHS[pathType];
  } else {
    fullFilePaths = `sitemap_pSEO/`;
  }
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
    console.log(process.env.accessKeyId);
    console.log(process.env.region);
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

const generateSitemap = async (type) => {
  const MAX = 5000;
  const locations = getStaticLocations();
  const roles = await fetchRoles();
  let urls = [];

  if (type === 'role') urls = roles.map(r => `https://www.jobtrees.com/browse-careers/${formatRole(r)}`);
  else if (type === 'city') urls = locations.map(({ city, stateAbbr }) => `https://www.jobtrees.com/browse-careers/${formatCity(city)}-${stateAbbr}`);
  else if (type === 'company') urls = locations.map(({ city, stateAbbr }) => `https://www.jobtrees.com/top-companies/${formatCity(city)}-${stateAbbr}`);
  else urls = roles.flatMap(r => locations.map(({ city, stateAbbr }) => buildUrl(r, city, stateAbbr)));

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
  ? 'index_browse_company.xml'
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
  console.log(`${type} sitemap complete.`);
};

(async () => {
  await Promise.all([
    generateSitemap('role'),
    generateSitemap('city'),
    generateSitemap('company'),
    generateSitemap('all'),
  ]);
})();
