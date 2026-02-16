"use client";

import { InstantSearchNext } from "react-instantsearch-nextjs";
import {
	SearchBox,
	Hits,
	RefinementList,
	DynamicWidgets,
} from "react-instantsearch";
import { client } from "../libs/client";
import { Hit } from "./Hit";
import { QueryId } from "./QueryId";
import { Panel } from "./Panel";

export function Search() {
	return (
		<InstantSearchNext
			searchClient={client}
			indexName="test_MSc"
			routing
			insights={false}
		>
			<div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
				<div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
					<aside className="lg:col-span-1">
						<div className="sticky top-20">
							<h2 className="text-lg font-semibold mb-4 text-gray-900 dark:text-gray-100">
								Filters
							</h2>
							<DynamicWidgets fallbackComponent={FallbackComponent} />
						</div>
					</aside>
					<main className="lg:col-span-3 bg-white dark:bg-gray-700 p-6 rounded-lg shadow-sm">
						<div className="mb-6">
							<SearchBox placeholder="Search for blueprints" />
						</div>
						<Hits hitComponent={Hit} />
					</main>
				</div>
			</div>
			<QueryId />
		</InstantSearchNext>
	);
}

function FallbackComponent({ attribute }: { attribute: string }) {
	return (
		<Panel header={attribute}>
			<RefinementList attribute={attribute} />
		</Panel>
	);
}
