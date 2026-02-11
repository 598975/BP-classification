"use client";
import Link from "next/link";
import { Blueprint } from "../interfaces";

type RelatedBlueprints = {
	blueprints: Blueprint[];
};

export const RelatedBlueprints = ({ blueprints }: RelatedBlueprints) => {
	if (blueprints.length === 0) {
		return (
			<div className="mt-8">
				<h2 className="text-2xl font-bold mb-4">Related Blueprints</h2>
				<p className="text-gray-500">No related blueprints found.</p>
			</div>
		);
	}

	return (
		<div className="mt-8">
			<h2 className="text-2xl font-bold mb-4">Related Blueprints</h2>
			<div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
				{blueprints.map((bp) => (
					<Link
						key={bp.id.toString()}
						href={`/blueprint/${bp.id}`}
						className="p-4 bg-gray-100 dark:bg-gray-800 rounded-lg hover:bg-gray-200 dark:hover:bg-gray-700 transition-colors"
					>
						<h3 className="font-semibold text-lg mb-2 line-clamp-1">
							{bp.name || (
								<span className="italic text-gray-400">Unnamed Blueprint</span>
							)}
						</h3>
						<p className="text-gray-600 dark:text-gray-400 text-sm line-clamp-2">
							{bp.description || "No description available."}
						</p>
					</Link>
				))}
			</div>
		</div>
	);
};
