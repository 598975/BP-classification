import { prisma } from '../libs/prisma'

async function main() {
  // Example: Fetch all records from a table
  // Replace 'user' with your actual model name
  const allBps = await prisma.blueprints_categorized.findMany()
  console.log('All Bps:', JSON.stringify(allBps, null, 2))
}

main()
  .then(async () => {
    await prisma.$disconnect()
  })
  .catch(async (e) => {
    console.error(e)
    await prisma.$disconnect()
    process.exit(1)
  })