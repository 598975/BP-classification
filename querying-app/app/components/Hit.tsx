import { Hit as AlgoliaHit } from "instantsearch.js";
import { Highlight } from "react-instantsearch";
import Link from "next/link";

type HitProps = {
	hit: AlgoliaHit<{
		name: string;
		description?: string;
	}>;
};

export function Hit({ hit }: HitProps) {
	return (
		<article className="hit-item border-b border-gray-200 dark:border-gray-700 py-4 px-2">
			<div className="flex flex-col gap-2 bg-white dark:bg-gray-800 p-4 rounded-lg shadow-sm hover:shadow-md transition-shadow">
				<h3 className="text-lg font-semibold text-gray-900 dark:text-gray-100">
					<Highlight hit={hit} attribute="name" />
				</h3>

				{hit.description && (
					<p className="text-sm text-gray-600 dark:text-gray-400 line-clamp-2">
						<Highlight hit={hit} attribute="description" />
					</p>
				)}

				<div className="flex gap-2 mt-2">
					{hit.objectID && (
						<Link
							href={`/blueprint/${hit.objectID}`}
							target="_blank"
							rel="noopener noreferrer"
							className="text-sm text-green-600 dark:text-green-400 hover:underline"
						>
							View Blueprint →
						</Link>
					)}
				</div>
			</div>
		</article>
	);
}
