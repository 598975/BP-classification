import { createMemoryCache } from "@algolia/client-common";
import { liteClient } from "algoliasearch/lite";

const appId = process.env.NEXT_PUBLIC_ALGOLIA_APP_ID || "";
const apiKey = process.env.NEXT_PUBLIC_SEARCH_API_KEY || "";

export const responsesCache = createMemoryCache();
export const client = liteClient(appId, apiKey, {
	responsesCache,
});
