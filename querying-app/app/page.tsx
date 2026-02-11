import { Suspense } from "react";
import { BlueprintList } from "./components/BlueprintList";
import { getAllBps } from "./api/script";

export default async function Home() {
	const data = await getAllBps();

	return (
		<div className="flex min-h-screen items-center justify-center bg-zinc-50 font-sans dark:bg-black">
			<main className="flex min-h-screen w-full flex-col py-32 px-8 bg-white dark:bg-black">
				<Suspense fallback={<div>Loading...</div>}>
					<BlueprintList initialBlueprints={data} />
				</Suspense>
			</main>
		</div>
	);
}
