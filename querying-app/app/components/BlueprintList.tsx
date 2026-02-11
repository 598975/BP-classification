"use client";

import { useSearchParams } from "next/navigation";
import { useState, useEffect } from "react";
import { Blueprint } from "../interfaces";
import { BlueprintCard } from "./BlueprintCard";
import { SearchInput } from "./SearchInput";
import { matchesQuery } from "../libs/searchUtils";

type BlueprintListProps = {
	initialBlueprints: Blueprint[];
};

export function BlueprintList({ initialBlueprints }: BlueprintListProps) {
	const searchParams = useSearchParams();
	const searchQuery = searchParams && searchParams.get("q");
	const parsedQueryParam = searchParams && searchParams.get("parsed");

	const [blueprintData, setBlueprintData] =
		useState<Blueprint[]>(initialBlueprints);

	const totalBlueprints = blueprintData.length;

	useEffect(() => {
		const handleSearch = () => {
			if (!searchQuery) {
				setBlueprintData(initialBlueprints);
				return;
			}

			if (parsedQueryParam) {
				try {
					const parsedQuery = JSON.parse(parsedQueryParam);
					const filteredBlueprints = initialBlueprints.filter((bp) =>
						matchesQuery(bp, parsedQuery),
					);
					setBlueprintData(filteredBlueprints);
					return;
				} catch (error) {
					console.error("Error parsing query:", error);
				}
			}

			const findBlueprints = initialBlueprints.filter((bp) => {
				if (searchQuery) {
					return (
						bp.name?.toLowerCase().includes(searchQuery.toLowerCase()) ||
						bp.description?.toLowerCase().includes(searchQuery.toLowerCase())
					);
				} else {
					return true;
				}
			});
			setBlueprintData(findBlueprints);
		};
		handleSearch();
	}, [searchQuery, parsedQueryParam, initialBlueprints]);

	return (
		<>
			<p className="mb-10 ">
				Showing {totalBlueprints}{" "}
				{totalBlueprints > 1 ? "Blueprints" : "Blueprint"}
			</p>

			<SearchInput defaultValue={searchQuery} />

			<div className="mt-8">
				{totalBlueprints === 0 ? (
					<p>No result returned</p>
				) : (
					<div className="grid grid-cols-1 md:grid-cols-3 items-center gap-5">
						{blueprintData.map((blueprint: Blueprint) => {
							return (
								<div key={blueprint.id.toString()}>
									<BlueprintCard
										name={blueprint.name}
										description={blueprint.description}
										created_at={blueprint.created_at}
										id={blueprint.id}
									/>
								</div>
							);
						})}
					</div>
				)}
			</div>
		</>
	);
}
