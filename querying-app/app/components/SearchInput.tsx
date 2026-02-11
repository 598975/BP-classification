import { useRouter } from "next/navigation";
import { useState, ChangeEvent } from "react";
import { ParsedQuery } from "../interfaces";

interface iDefault {
	defaultValue: string | null;
}

export const SearchInput = ({ defaultValue }: iDefault) => {
	const router = useRouter();

	const [inputValue, setValue] = useState(defaultValue);

	const handleChange = (event: ChangeEvent<HTMLInputElement>) => {
		const inputValue = event.target.value;
		setValue(inputValue);
	};

	const parseSearchQuery = (query: string) => {
		const tokens: string[] = [];
		let currentToken = "";
		let inQuotes = false;
		let quoteChar = "";

		for (let i = 0; i < query.length; i++) {
			const char = query[i];

			if ((char === '"' || char === "'") && !inQuotes) {
				inQuotes = true;
				quoteChar = char;
				currentToken += char;
			} else if (char === quoteChar && inQuotes) {
				inQuotes = false;
				currentToken += char;
				tokens.push(currentToken);
				currentToken = "";
			} else if ((char === " " || char === "(" || char === ")") && !inQuotes) {
				if (currentToken) tokens.push(currentToken);
				if (char === "(" || char === ")") tokens.push(char);
				currentToken = "";
			} else {
				currentToken += char;
			}
		}
		if (currentToken) tokens.push(currentToken);

		// Parse tokens into tree structure
		return parseTokens(tokens);
	};

	const parseTokens = (tokens: string[]): ParsedQuery => {
		const output: ParsedQuery[] = [];
		const operators: string[] = [];

		let i = 0;
		while (i < tokens.length) {
			const token = tokens[i];
			const upper = token.toUpperCase();

			if (token === "(") {
				let depth = 1;
				let j = i + 1;
				const subTokens: string[] = [];

				while (j < tokens.length && depth > 0) {
					if (tokens[j] === "(") depth++;
					if (tokens[j] === ")") depth--;
					if (depth > 0) subTokens.push(tokens[j]);
					j++;
				}

				output.push({ type: "GROUP", children: [parseTokens(subTokens)] });
				i = j;
				continue;
			}

			if (upper === "NOT") {
				i++;
				if (i < tokens.length) {
					const nextToken = tokens[i];
					if (nextToken.startsWith('"') || nextToken.startsWith("'")) {
						output.push({
							type: "PHRASE",
							value: nextToken.slice(1, -1),
							negated: true,
						});
					} else {
						output.push({ type: "TERM", value: nextToken, negated: true });
					}
				}
				i++;
				continue;
			}

			if (upper === "&&" || upper === "||") {
				operators.push(upper === "&&" ? "AND" : "OR");
				i++;
				continue;
			}

			if (token.startsWith('"') || token.startsWith("'")) {
				output.push({ type: "PHRASE", value: token.slice(1, -1) });
				i++;
				continue;
			}

			if (token !== "(" && token !== ")") {
				output.push({ type: "TERM", value: token });
			}
			i++;
		}

		if (operators.length === 0 && output.length === 1) {
			return output[0];
		}

		const uniqueOperators = [...new Set(operators)];
		if (uniqueOperators.length === 1) {
			return {
				type: uniqueOperators[0] as "AND" | "OR",
				children: output,
			};
		}

		return {
			type: "OR",
			children: groupByOperator(output, operators),
		};
	};

	const groupByOperator = (
		terms: ParsedQuery[],
		operators: string[],
	): ParsedQuery[] => {
		const result: ParsedQuery[] = [];
		let current: ParsedQuery[] = [terms[0]];

		for (let i = 0; i < operators.length; i++) {
			if (operators[i] === "&&") {
				current.push(terms[i + 1]);
			} else {
				if (current.length > 1) {
					result.push({ type: "AND", children: current });
				} else {
					result.push(current[0]);
				}
				current = [terms[i + 1]];
			}
		}

		if (current.length > 1) {
			result.push({ type: "AND", children: current });
		} else if (current.length === 1) {
			result.push(current[0]);
		}

		return result;
	};

	const handleSearch = () => {
		if (inputValue) {
			const parsedQuery = parseSearchQuery(inputValue);
			const params = new URLSearchParams({
				q: inputValue,
				parsed: JSON.stringify(parsedQuery),
			});
			return router.push(`/?q=${params.toString()}`);
		}

		if (!inputValue) return router.push("/");
	};

	const handleKeyPress = (event: { key: unknown }): void => {
		if (event.key === "Enter") return handleSearch();
	};

	return (
		<div className="space-y-2">
			<div className="search__input border-2 border-solid border-slate-500 flex flex-row items-center gap-5 p-1 rounded-[15px]">
				<label htmlFor="inputId" className="pl-3 cursor-pointer">
					<svg
						xmlns="http://www.w3.org/2000/svg"
						className="h-5 w-5 text-gray-500"
						fill="none"
						viewBox="0 0 24 24"
						stroke="currentColor"
					>
						<path
							strokeLinecap="round"
							strokeLinejoin="round"
							strokeWidth={2}
							d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
						/>
					</svg>
				</label>

				<input
					type="text"
					id="inputId"
					placeholder="Enter your keywords"
					value={inputValue ?? ""}
					onChange={handleChange}
					onKeyDown={handleKeyPress}
					className="bg-transparent outline-none border-none w-full py-3 pl-2 pr-3"
				/>
			</div>

			<div className="text-xs text-gray-500 dark:text-gray-400 px-3 space-y-1">
				<div>
					<span className="font-semibold">&&</span> - match all terms
				</div>
				<div>
					<span className="font-semibold">||</span> - match any term
				</div>
				<div>
					<span className="font-semibold">NOT</span> - exclude term
				</div>
				<div>
					<span className="font-semibold">&quot;phrase&quot;</span> - exact
					phrase match
				</div>
				<div>
					<span className="font-semibold">()</span> - group conditions
				</div>
			</div>
		</div>
	);
};
