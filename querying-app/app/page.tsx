import { Suspense } from "react";
import { numberOfCategorizedBps, numberOfTotalBps } from "./api/script";

export default async function Home() {
  const total_bps = await numberOfTotalBps();
  const categorized_bps = await numberOfCategorizedBps();

  return (
    <div className="flex min-h-screen items-center justify-center bg-zinc-50 font-sans dark:bg-black">
      <main className="flex min-h-screen w-full flex-col py-32 px-8 bg-white dark:bg-black">
        <Suspense fallback={<div>Loading...</div>}></Suspense>
        <p className="mb-4 text-lg text-gray-700 dark:text-gray-300">
          Search instructions: <br />
          The search uses an <code>AND</code> operator, meaning that if you
          search <code>motion light</code>, this will be interpreted as{" "}
          <code>motion AND light</code>. <br />
          Grouped phrases are accepted, so you can search{" "}
          <code>"motion light"</code>, and it will look for the combined phrase{" "}
          <code>motion light</code>. <br />
          Negated terms are also supported, so if you search{" "}
          <code>motion -light</code>, it will look for blueprints that contain{" "}
          <code>motion</code> but do not contain <code>light</code>. <br />
        </p>
        <p className="fixed bottom-0 left-0 w-full bg-zinc-100 dark:bg-zinc-900 p-4 text-sm text-center">
          Home Assistant blueprints last retrieved: 28/08/2025 <br />
          Total number of blueprints: {total_bps} <br />
          Number of blueprints after filtering: {categorized_bps} <br />
        </p>
      </main>
    </div>
  );
}
