import { S3Client, PutObjectCommand, ListObjectsV2Command, DeleteObjectCommand } from '@aws-sdk/client-s3';
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

const s3Client = new S3Client({ region, credentials });

const getStaticLocations = () => [
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


const getCurrentDateString = () => {
  const date = new Date();
  return date.toISOString().split('T')[0]; // format as YYYY-MM-DD
};

const generateSitemapXml = async (isOdd) => {
  const MAX_URLS_PER_SITEMAP = 5000;
  const locations = getStaticLocations();
  const roles = await fetchRoles();
  const urls = roles.flatMap(role => locations.map(({ city, stateAbbr }) => generateUrl(role, city, stateAbbr)));
  
  const currentDate = getCurrentDateString(); // Get the current date to append to the filenames
  let sitemapFiles = [];
  
  const identifier = isOdd ? "odd" : "even";  // Use identifier for odd or even
  let sitemapIndex = isOdd ? 1 : 2;  // Start from 1 for odd, 2 for even
  
  while (urls.length > 0) {
    const batch = urls.splice(0, MAX_URLS_PER_SITEMAP);
    const sitemapFileName = `pSEO_page_${sitemapIndex}_${identifier}_${currentDate}.xml`; // Append date and identifier
    await limit(() => uploadToS3(sitemapFileName, generateSitemapXmlContent(batch)));
    sitemapFiles.push(sitemapFileName);
    sitemapIndex += 2;
  }
  
  // await deleteOldSitemapFiles(sitemapFiles, currentDate, identifier); // Delete only today's files with correct identifier
  await updateSitemapIndex(sitemapFiles, currentDate); // Update sitemap index with today's files
};

const deleteOldSitemapFiles = async (newFiles, date, identifier) => {
  const existingFiles = await getExistingSitemapFiles(date, identifier);
  const oldFiles = existingFiles.filter(file => !newFiles.includes(file));
  
  await Promise.all(oldFiles.map(file => 
    limit(() => s3Client.send(new DeleteObjectCommand({ Bucket: S3_BUCKET, Key: `${S3_PUBLIC_PATH}${file}` })))
  ));
};

const getExistingSitemapFiles = async () => {
  try {
    console.log('Fetching existing sitemaps...');
    const response = await s3Client.send(new ListObjectsV2Command({ Bucket: S3_BUCKET, Prefix: S3_PUBLIC_PATH }));
    return response.Contents ? response.Contents.map(item => item.Key).filter(file => !file.includes('sitemap_index_pSEO.xml')) : [];
  } catch (error) {
    console.error('Error fetching existing sitemaps:', error);
    return [];
  }
};


const updateSitemapIndex = async (newFiles, date) => {
  // Fetch existing sitemap files for the given date
  const existingFiles = await getExistingSitemapFiles(date);

  // Merge new files with existing files, avoiding duplicates
  const allFiles = [...new Set([...existingFiles, ...newFiles])];
  
  // Upload the updated sitemap index
  await limit(() => uploadToS3(`sitemap_index_pSEO.xml`, generateSitemapIndex(allFiles, date))); // Always update the main index file
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

const generateSitemapIndex = (sitemapFiles, date) => {
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

// Call these methods separately for odd and even
(async () => {
  // await generateSitemapXml(true);  // Odd
  await generateSitemapXml(false); // Even
})();
