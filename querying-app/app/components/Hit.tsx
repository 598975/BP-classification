import { Hit as AlgoliaHit } from "instantsearch.js";
import { Highlight } from "react-instantsearch";

type HitProps = {
	hit: AlgoliaHit<{
		name: string;
		description?: string;
		topic_title?: string;
		topic_url?: string;
		blueprint_url?: string;
		username?: string;
		views?: number;
		like_count?: number;
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
				
				{hit.topic_title && (
					<p className="text-sm text-gray-500 dark:text-gray-500">
						Topic: <Highlight hit={hit} attribute="topic_title" />
					</p>
				)}
				
				<div className="flex gap-4 text-xs text-gray-500 dark:text-gray-400">
					{hit.username && <span>By {hit.username}</span>}
					{hit.views && <span>{hit.views} views</span>}
					{hit.like_count && <span>{hit.like_count} likes</span>}
				</div>
				
				<div className="flex gap-2 mt-2">
					{hit.topic_url && (
						<a
							href={hit.topic_url}
							target="_blank"
							rel="noopener noreferrer"
							className="text-sm text-blue-600 dark:text-blue-400 hover:underline"
						>
							View Discussion →
						</a>
					)}
					{hit.blueprint_url && (
						<a
							href={hit.blueprint_url}
							target="_blank"
							rel="noopener noreferrer"
							className="text-sm text-green-600 dark:text-green-400 hover:underline"
						>
							View Blueprint →
						</a>
					)}
				</div>
			</div>
		</article>
	);
}
