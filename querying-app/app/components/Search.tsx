"use client";

import { InstantSearchNext } from "react-instantsearch-nextjs";
import {
	SearchBox,
	Hits,
	RefinementList,
	Pagination,
	HitsPerPage,
	Stats,
	ClearRefinements,
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
						<div className="sticky top-20 max-h-[calc(100vh-6rem)] overflow-y-auto p-4 bg-white dark:bg-gray-700 rounded-lg shadow-sm">
							<h2 className="text-lg font-semibold mb-4 text-gray-900 dark:text-gray-100">
								Filters
							</h2>

							<Panel>
								<RefinementList
									attribute="features"
									searchable={true}
									searchablePlaceholder="Search for feature"
									showMore={true}
									operator="and"
								/>
								<ClearRefinements />
							</Panel>
						</div>
					</aside>
					<main className="lg:col-span-3 bg-white dark:bg-gray-700 p-6 rounded-lg shadow-sm">
						<div className="mb-6">
							<SearchBox placeholder="Search for blueprints" />
						</div>

						<div className="mb-4 flex justify-between items-center gap-2">
							<Stats />
							<span className="text-sm text-gray-700 dark:text-gray-300">
								Results per page:
								<HitsPerPage
									items={[
										{ label: "10", value: 10, default: true },
										{ label: "20", value: 20 },
										{ label: "50", value: 50 },
										{ label: "100", value: 100 },
									]}
								/>
							</span>
						</div>
						<Hits hitComponent={Hit} />
						<div className="mt-8 flex justify-center">
							<Pagination
								padding={2}
								showFirst={true}
								showLast={true}
								classNames={{
									root: "flex gap-2",
									list: "flex gap-1",
									item: "px-3 py-2 rounded border border-gray-300 dark:border-gray-600 hover:bg-gray-100 dark:hover:bg-gray-600",
									selectedItem: "bg-blue-500 text-white border-blue-500",
									disabledItem: "opacity-50 cursor-not-allowed",
									link: "block",
								}}
							/>
						</div>
					</main>
				</div>
			</div>
			<QueryId />
		</InstantSearchNext>
	);
}
