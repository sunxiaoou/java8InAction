package lambdasinaction.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import static io.netty.buffer.ByteBufUtil.appendPrettyHexDump;
import static io.netty.util.internal.StringUtil.NEWLINE;

public class TestByteBuf {
    private static void log(ByteBuf buf) {
        int length = buf.readableBytes();
        int rows = length / 16 + (length % 15 == 0 ? 0: 1) + 4;
        StringBuilder sb = new StringBuilder(rows * 80 * 2)
                .append("read index:").append(buf.readerIndex())
                .append(" write index:").append(buf.writerIndex())
                .append(" capacity:").append(buf.capacity())
                .append(NEWLINE);
        appendPrettyHexDump(sb, buf);
        System.out.println(sb.toString());
    }

    public static void main(String[] args) {
        ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();
        log(buf);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 300; i ++) {
            sb.append("a");
        }
        buf.writeBytes(sb.toString().getBytes());
        log(buf);

        buf = ByteBufAllocator.DEFAULT.buffer(10);
        System.out.println(buf);
        buf.writeBytes(new byte[]{1, 2, 3, 4});
        buf.writeInt(42);
        log(buf);
        buf.writeInt(43);
        log(buf);
        System.out.println(buf.readBytes(4));
        System.out.println(buf.readInt());
        log(buf);
    }
}
