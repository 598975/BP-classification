import React from "react";

export function Panel({
	children,
	header,
	footer,
}: {
	children: React.ReactNode;
	header?: React.ReactNode;
	footer?: React.ReactNode;
}) {
	return (
		<div className="mb-6 border-b border-gray-200 dark:border-gray-700 pb-4">
			{header && (
				<div className="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-3 capitalize">
					{header}
				</div>
			)}
			<div className="text-sm">{children}</div>
			{footer && (
				<div className="mt-3 text-xs text-gray-500 dark:text-gray-400">
					{footer}
				</div>
			)}
		</div>
	);
}
