import * as prisma from "@/app/libs/prisma";

(BigInt.prototype as any).toJSON = function () {
	return this.toString();
};

export async function getUniqueClusters() {
	const blueprints = await prisma.prisma.blueprints_categorized.findMany({
		select: {
			fine_cluster: true,
			top_cluster: true,
		},
	});

	const clusterMap = new Map<string, Set<string>>();

	blueprints.forEach((bp) => {
		if (bp.top_cluster !== null && bp.fine_cluster !== null) {
			const topKey = bp.top_cluster.toString();

			if (!clusterMap.has(topKey)) {
				clusterMap.set(topKey, new Set());
			}

			clusterMap.get(topKey)!.add(bp.fine_cluster.toString());
		}
	});

	const topClusters = Array.from(clusterMap.entries())
		.map(([topCluster, fineClusters]) => ({
			topCluster: BigInt(topCluster),
			fineClusters: Array.from(fineClusters)
				.map((fc) => BigInt(fc))
				.sort((a, b) => Number(a) - Number(b)),
		}))
		.sort((a, b) => Number(a.topCluster) - Number(b.topCluster));

	return {
		topClusters,
		allFineClusters: Array.from(
			new Set(
				blueprints.map((bp) => bp.fine_cluster).filter((fc) => fc !== null),
			),
		).sort((a, b) => Number(a!) - Number(b!)),
	};
}

export async function getAllBps() {
	const allBps = await prisma.prisma.blueprints_categorized.findMany();

	return allBps;
}


