"use client";
import Link from "next/link";

type BlueprintCardProps = {
	name: string | null;
	description: string | null;
	created_at: Date | string | null;
	id: bigint;
};

export const BlueprintCard = (props: BlueprintCardProps) => {
	if (!props) {
		return <div>No blueprint data available.</div>;
	}

	const { description, name, created_at, id } = props;

	const maxDescriptionLength = 100;
	const truncatedDescription = description
		? description.length > maxDescriptionLength
			? description.substring(0, maxDescriptionLength) + "..."
			: description
		: "N/A";

	return (
		<div className="blueprint__card rounded-[15px] border border-solid">
			<div className=" bg-gray-500 p-3 rounded-[15px] hover:bg-gray-700 transition-colors duration-300">
				<Link href={`/blueprint/${id}`} rel="noopener noreferrer">
					<h2 className="">Name: {name}</h2>

					<p>Description: {truncatedDescription}</p>

					<p>
						Created at:{" "}
						{created_at ? new Date(created_at).toDateString() : "N/A"}
					</p>
				</Link>
			</div>
		</div>
	);
};
