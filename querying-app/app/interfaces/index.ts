import { prisma } from "@/app/libs/prisma";
import { Hit as AlgoliaHit } from "instantsearch.js";


(BigInt.prototype as any).toJSON = function () {
	return this.toString();
};

export type Post = Awaited<ReturnType<typeof prisma.posts.findFirst>>;

export type Topic = Awaited<ReturnType<typeof prisma.topics.findFirst>>;

export type Blueprint = NonNullable<
	Awaited<ReturnType<typeof prisma.blueprints_categorized.findFirst>>
>;

export type BpWithRelations = Blueprint & {
	post: Post | undefined;
	topic: Topic | undefined;
};

export type HitProps = {
	hit: AlgoliaHit<{
		name: string;
		description?: string;
	}>;
};
