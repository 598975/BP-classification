import { Search } from "../components/Search";
import { responsesCache } from "../libs/client";

export const dynamic = "force-dynamic";

export default function SearchPage() {
	responsesCache.clear();

	return <Search />;
}
