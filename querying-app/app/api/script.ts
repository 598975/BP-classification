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

export async function getUniqueCategories() {
  const blueprints = await prisma.prisma.blueprints_categorized.findMany({
    select: {
      category: true,
      sub_category: true,
    },
  });

  const categoryMap = new Map<string, Set<string>>();

  blueprints.forEach((bp) => {
    if (bp.category !== null && bp.sub_category !== null) {
      const categoryKey = bp.category;

      if (!categoryMap.has(categoryKey)) {
        categoryMap.set(categoryKey, new Set());
      }

      categoryMap.get(categoryKey)!.add(bp.sub_category);
    }
  });

  const categories = Array.from(categoryMap.entries())
    .map(([category, subCategories]) => ({
      category,
      subCategories: Array.from(subCategories).sort(),
    }))
    .sort((a, b) => a.category.localeCompare(b.category));

  return categories;
}

export async function getAllBps() {
  const allBps = await prisma.prisma.blueprints_categorized.findMany();

  return allBps;
}

export async function numberOfCategorizedBps() {
  const count = await prisma.prisma.blueprints_categorized.count();

  return count;
}

export async function numberOfTotalBps() {
  const count = await prisma.prisma.blueprints.count();

  return count;
}
