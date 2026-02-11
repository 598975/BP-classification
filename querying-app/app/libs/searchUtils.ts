import { Blueprint, ParsedQuery } from "../interfaces";

export const matchesQuery = (
	bp: Blueprint,
	query: ParsedQuery,
	searchFields: string[] = ["name", "description", "keywords_yake"],
): boolean => {
	const searchText = searchFields
		.map((field) => bp[field as keyof Blueprint])
		.join(" ")
		.toLowerCase();

	switch (query.type) {
		case "TERM":
			const matches = searchText.includes(query.value!.toLowerCase());
			return query.negated ? !matches : matches;

		case "PHRASE":
			const phraseMatches = searchText.includes(query.value!.toLowerCase());
			return query.negated ? !phraseMatches : phraseMatches;

		case "AND":
			return query.children!.every((child) =>
				matchesQuery(bp, child, searchFields),
			);

		case "OR":
			return query.children!.some((child) =>
				matchesQuery(bp, child, searchFields),
			);

		case "NOT":
			return !matchesQuery(bp, query.children![0], searchFields);

		case "GROUP":
			return matchesQuery(bp, query.children![0], searchFields);

		default:
			return true;
	}
};
