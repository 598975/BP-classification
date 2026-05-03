import { getUniqueClusters, getAllBps, getUniqueCategories } from "../api/script";
import Link from "next/link";
import { connection } from "next/server";

export default async function BlueprintsPage() {
  await connection(); // Ensure the database connection is established before rendering
  const blueprints = await getAllBps();
  // const clusters = await getUniqueClusters();
  const categories = await getUniqueCategories();

  return (
    <div className="flex min-h-screen items-center justify-center bg-zinc-50 font-sans dark:bg-black">
      <main className="flex min-h-screen w-full flex-col py-32 px-8 bg-white dark:bg-black">
        <h1 className="text-4xl font-bold tracking-tight text-gray-700 dark:text-gray-200 mb-8">
          Blueprint Category Explorer
        </h1>

        <div className="space-y-2 w-[60%] min-w-150">
          {categories.map((category) => (
            <details key={category.category} className="group">
              <summary className="cursor-pointer p-4 bg-blue-100 dark:bg-blue-900 rounded-lg hover:bg-blue-200 dark:hover:bg-blue-800 font-semibold text-lg">
                {category.category}
                <span className="text-sm font-normal ml-2 text-gray-600 dark:text-gray-400">
                  ({category.subCategories.length} sub-categories)
                </span>
              </summary>

              <div className="ml-4 mt-2 space-y-2">
                {category.subCategories.map((subCategory) => (
                  <details key={subCategory} className="group">
                    <summary className="cursor-pointer p-3 bg-green-100 dark:bg-green-900 rounded-lg hover:bg-green-200 dark:hover:bg-green-800 font-medium">
                      {subCategory}
                      <span className="text-sm font-normal ml-2 text-gray-600 dark:text-gray-400">
                        (
                        {
                          blueprints.filter(
                            (bp) => bp?.sub_category === subCategory,
                          ).length
                        }{" "}
                        blueprints)
                      </span>
                    </summary>

                    <ul className="ml-4 mt-2 space-y-1">
                      {blueprints
                        .filter((bp) => bp.sub_category === subCategory)
                        .map((bp) => (
                          <li
                            key={bp.id}
                            className="p-2 text-gray-700 dark:text-gray-300 hover:bg-gray-100 dark:hover:bg-gray-800 rounded truncate"
                          >
                            <Link
                              href={`/blueprint/${bp.id}`}
                              className="hover:underline"
                              rel="noopener noreferrer"
                            >
                              {bp.name || (
                                <span className="italic text-gray-400">
                                  Unnamed Blueprint
                                </span>
                              )}
                            </Link>
                          </li>
                        ))}
                    </ul>
                  </details>
                ))}
              </div>
            </details>
          ))}
        </div>
      </main>
    </div>
  );
}
