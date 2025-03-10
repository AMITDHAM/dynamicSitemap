import { S3Client, ListObjectsV2Command } from "@aws-sdk/client-s3";
import aws4 from "aws4";
import dotenv from "dotenv";
import pLimit from "p-limit";
import fs from "fs";

dotenv.config({ path: ".env.local" });
const limit = pLimit(20); // Reduce parallelism to control memory usage

const region = process.env.region;
const credentials = {
  accessKeyId: process.env.accessKeyId,
  secretAccessKey: process.env.secretAccessKey,
};
const OPEN_SEARCH_URL = process.env.OPEN_SEARCH_URL;

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

const formatRoleName = (role) =>
  role.toLowerCase().replace(/-/g, "--").replace(/ /g, "-");
const formatCityName = (city) => city.toLowerCase().replace(/ /g, "-");
const generateUrl = (role, city, stateAbbr) =>
  `https://www.jobtrees.com/browse-careers/${formatRoleName(
    role
  )}-jobs-in-${formatCityName(city)}-${stateAbbr.toLowerCase()}`;

const fetchRoles = async () => {
  try {
    console.log("Fetching roles...");
    const response = await fetch("https://api.jobtrees.com/roles/roleList");
    const data = await response.json();
    console.log("Roles fetched successfully:", data.length);
    return data;
  } catch (error) {
    console.error("Error fetching roles:", error);
    return [];
  }
};

const checkJobExists = async (role, city, state) => {
  console.log(`Checking if job exists for ${role} in ${city}, ${state}...`);
  const indices = [
    "adzuna_postings",
    "big_job_site_postings",
    "indeed_jobs_postings",
    "jobtrees_postings",
    "greenhouse_postings",
  ];

  for (const indexName of indices) {
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
      service: "es",
      region,
      method: "POST",
      body: JSON.stringify(searchRequestBody),
      headers: { "Content-Type": "application/json" },
    };

    aws4.sign(searchRequest, credentials);

    try {
      const searchResponse = await fetch(
        `https://${searchRequest.host}${searchRequest.path}`,
        {
          method: searchRequest.method,
          headers: searchRequest.headers,
          body: searchRequest.body,
        }
      );

      const searchResponseBody = await searchResponse.json();
      const jobCount = searchResponseBody.hits?.total?.value || 0;

      if (jobCount > 0) return true; // Stop early if any job exists
    } catch (error) {
      console.error(
        `Error checking jobs in "${indexName}" for "${role}" in "${city}, ${state}":`,
        error
      );
    }
  }

  return false;
};

const generateNoJobUrlsFile = async () => {
  const locations = getStaticLocations();
  const roles = await fetchRoles();
  const fileStream = fs.createWriteStream("no_job_urls.txt", { flags: "w" });

  for (const role of roles) {
    for (const { city, state, stateAbbr } of locations) {
      await limit(async () => {
        const exists = await checkJobExists(role, city, state);
        if (!exists) {
          const url = generateUrl(role, city, stateAbbr);
          fileStream.write(url + "\n");
          console.log("No job found, added:", url);
        }
      });
    }
  }

  fileStream.end();
  console.log("No job URLs written to no_job_urls.txt");
};

(async () => {
  await generateNoJobUrlsFile();
})();
