import type { Blueprint } from "@/app/interfaces";
import { prisma } from "@/app/libs/prisma";
import { notFound, redirect } from "next/navigation";
import { features } from "process";
import { parse } from "yaml";

export default async function BlueprintPage({
	params,
}: {
	params: Promise<{ id: string }>;
}) {
	const { id } = await params;
	const bpId = BigInt(id);

	const bp = await prisma.blueprints_categorized.findUnique({
		where: {
			id: bpId,
		},
	});

	if (!bp) {
		notFound();
	}

	return (
		<div className="flex min-h-screen items-center justify-center bg-zinc-50 font-sans dark:bg-black">
			<main className="flex min-h-screen w-full flex-col py-32 px-8 bg-white dark:bg-black">
				<h1 className="text-4xl font-bold tracking-tight text-gray-700 dark:text-gray-200 mb-8">
					{bp.name || (
						<span className="italic text-gray-400">Unnamed Blueprint</span>
					)}
				</h1>
				<p className="text-gray-600 dark:text-gray-400 mb-4">
					{bp.description || "No description available."}
				</p>
				<div className="space-y-2">
					{Object.entries(bp)
						.filter(
							([key]) =>
								key !== "id" && key !== "name" && key !== "description",
						)
						.map(([key, value]) => {
							let displayValue: React.ReactNode;

							// Parse YAML for blueprint_code
							if (key === "blueprint_code" && typeof value === "string") {
								try {
									const parsed = parse(value, {
										customTags: [
											{
												tag: "!input",
												resolve: (str: string) => str,
											},
											{
												tag: "!secret",
												resolve: (str: string) => str,
											},
										],
									});
									displayValue = (
                                        <details className="cursor-pointer">
                                            <summary className="font-semibold text-blue-600 hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300">
                                                Click to expand blueprint code
                                            </summary>
                                            <pre className="mt-2 overflow-x-auto bg-gray-900 text-gray-100 p-4 rounded text-sm max-h-96 overflow-y-auto">
                                                {JSON.stringify(parsed, null, 2)}
                                            </pre>
                                        </details>
                                    );
								} catch (error) {
									// Fallback to raw YAML if parsing fails
									displayValue = (
										<details className="cursor-pointer">
											<summary className="font-semibold text-blue-600 hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300">
												Click to expand blueprint code
											</summary>
											<pre className="mt-2 overflow-x-auto bg-gray-900 text-gray-100 p-4 rounded text-sm whitespace-pre-wrap max-h-96 overflow-y-auto">
												{value}
											</pre>
										</details>
									);
								}
							} else if (key === "features" && typeof value === "string") {
								let featuresArray: string[] = value.split(" ");
								featuresArray = featuresArray.filter(
									(feature) => feature !== "<PAD>",
								);
								displayValue = featuresArray ? featuresArray.toString() : "N/A";
							} else {
								displayValue =
									value !== null && value !== undefined
										? value.toString()
										: "N/A";
							}

							return (
								<div
									key={key}
									className="p-4 bg-gray-100 dark:bg-gray-800 rounded-lg"
								>
									<h2 className="text-xl font-semibold text-gray-700 dark:text-gray-200 mb-2">
										{key
											.split("_")
											.map(
												(word) => word.charAt(0).toUpperCase() + word.slice(1),
											)
											.join(" ")}
									</h2>
									<div className="text-gray-600 dark:text-gray-400">
										{displayValue}
									</div>
								</div>
							);
						})}
				</div>
			</main>
		</div>
	);
}
