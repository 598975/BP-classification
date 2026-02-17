"use client";
import { RelatedProducts, InstantSearch, Carousel } from "react-instantsearch";
import { client } from "../libs/client";
import { Hit } from "./Hit";

export function RelatedBlueprints({ id }: { id: string }) {
	return (
		<div className="mt-8 p-6 bg-gray-100 dark:bg-gray-900 rounded-lg">
			<InstantSearch
				searchClient={client}
				indexName="test_MSc"
				insights={false}
			>
				<RelatedProducts
					objectIDs={[id]}
					layoutComponent={Carousel}
					itemComponent={({ item }) => <Hit hit={item} />}
					headerComponent={({ classNames, items }) => (
						<h2 className={classNames.title}>
							Recommendations ({items.length} items)
						</h2>
					)}
				/>
			</InstantSearch>
		</div>
	);
}
